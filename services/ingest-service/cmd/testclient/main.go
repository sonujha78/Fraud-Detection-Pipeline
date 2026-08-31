package main

import (
	"context"
	"log"
	"time"

	fraudv1 "github.com/frauddetection/ingest-service/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := fraudv1.NewIngestServiceClient(conn)

	req := &fraudv1.SubmitTransactionRequest{
		Transaction: &fraudv1.Transaction{
			TransactionId:   "grpc-test-001",
			UserId:          "user-grpc-1",
			CardId:          "card-grpc-1",
			Amount:          1500.00,
			Currency:        "INR",
			Merchant:        "TestMerchant",
			Location:        "IN-BLR",
			TransactionTime: timestamppb.New(time.Now()),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SubmitTransaction(ctx, req)
	if err != nil {
		log.Fatalf("SubmitTransaction failed: %v", err)
	}

	log.Printf("Response: accepted=%v message=%q", resp.GetAccepted(), resp.GetMessage())
}
