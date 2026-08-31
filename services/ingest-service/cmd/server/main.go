package main

import (
	"context"
	"log"
	"net"
	"os"
	"strings"
	"time"

	fraudv1 "github.com/frauddetection/ingest-service/proto/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const rawTopic = "transactions.raw"

// server implements the IngestService gRPC contract.
type server struct {
	fraudv1.UnimplementedIngestServiceServer
	writer *kafka.Writer
}

func (s *server) SubmitTransaction(ctx context.Context, req *fraudv1.SubmitTransactionRequest) (*fraudv1.SubmitTransactionResponse, error) {
	tx := req.GetTransaction()
	if tx == nil {
		return nil, status.Error(codes.InvalidArgument, "transaction is required")
	}
	if tx.GetTransactionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}
	if tx.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if tx.GetCardId() == "" {
		return nil, status.Error(codes.InvalidArgument, "card_id is required")
	}
	if tx.GetAmount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be positive")
	}

	payload, err := marshalTransactionJSON(tx)
	if err != nil {
		log.Printf("failed to marshal transaction %s: %v", tx.GetTransactionId(), err)
		return nil, status.Error(codes.Internal, "failed to encode transaction")
	}

	// IMPORTANT: key by card_id so all transactions for the same card land
	// on the same Kafka partition. The Streams app maintains per-card state
	// (last-known-location for the impossible-travel check); without a
	// consistent key, related transactions can be scattered across
	// partitions/tasks and the stateful check silently breaks.
	msg := kafka.Message{
		Key:   []byte(tx.GetCardId()),
		Value: payload,
		Time:  time.Now(),
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := s.writer.WriteMessages(writeCtx, msg); err != nil {
		log.Printf("failed to publish transaction %s to Kafka: %v", tx.GetTransactionId(), err)
		return nil, status.Error(codes.Unavailable, "failed to publish transaction")
	}

	return &fraudv1.SubmitTransactionResponse{
		Accepted: true,
		Message:  "transaction accepted for scoring",
	}, nil
}

func main() {
	bootstrapServers := getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092")
	grpcPort := getEnv("GRPC_PORT", "50051")

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(bootstrapServers, ",")...),
		Topic:                  rawTopic,
		Balancer:               &kafka.Hash{}, // key-based partitioning
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	fraudv1.RegisterIngestServiceServer(grpcServer, &server{writer: writer})

	log.Printf("Ingest Service listening on :%s, publishing to Kafka at %s", grpcPort, bootstrapServers)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve gRPC: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
