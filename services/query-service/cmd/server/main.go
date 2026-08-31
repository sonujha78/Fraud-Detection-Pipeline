package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	fraudv1 "github.com/frauddetection/query-service/proto/v1"
	"github.com/gocql/gocql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type server struct {
	fraudv1.UnimplementedQueryServiceServer
	session *gocql.Session
}

func (s *server) GetFlaggedTransactionsToday(ctx context.Context, req *fraudv1.GetFlaggedTransactionsTodayRequest) (*fraudv1.GetFlaggedTransactionsTodayResponse, error) {
	limit := req.GetLimit()
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	today := time.Now().UTC().Format("2006-01-02")

	query := s.session.Query(
		`SELECT transaction_id, user_id, card_id, CAST(amount AS DOUBLE) as amount, location, flagged_time, risk_score, risk_reason
		 FROM flagged_transactions WHERE flag_date = ? LIMIT ?`,
		today, limit,
	).WithContext(ctx)

	iter := query.Iter()
	defer iter.Close()

	var results []*fraudv1.ScoredTransaction
	var (
		transactionID, userID, cardID string
		amount                        float64
		location, riskReason          string
		flaggedTime                   time.Time
		riskScore                     float64
	)

	for iter.Scan(&transactionID, &userID, &cardID, &amount, &location, &flaggedTime, &riskScore, &riskReason) {
		results = append(results, &fraudv1.ScoredTransaction{
			TransactionId:   transactionID,
			UserId:          userID,
			CardId:          cardID,
			Amount:          amount,
			Location:        location,
			TransactionTime: timestamppb.New(flaggedTime),
			RiskScore:       riskScore,
			Flagged:         true,
			RiskReasons:     splitReasons(riskReason),
		})
	}

	if err := iter.Close(); err != nil {
		log.Printf("query error in GetFlaggedTransactionsToday: %v", err)
		return nil, status.Error(codes.Internal, "failed to query flagged transactions")
	}

	return &fraudv1.GetFlaggedTransactionsTodayResponse{Transactions: results}, nil
}

func (s *server) GetUserFraudScoreHistory(ctx context.Context, req *fraudv1.GetUserFraudScoreHistoryRequest) (*fraudv1.GetUserFraudScoreHistoryResponse, error) {
	userID := req.GetUserId()
	if userID == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	limit := req.GetLimit()
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	query := s.session.Query(
		`SELECT transaction_id, card_id, CAST(amount AS DOUBLE) as amount, merchant, location, transaction_time, currency
		 FROM transactions_by_user WHERE user_id = ? LIMIT ?`,
		userID, limit,
	).WithContext(ctx)

	iter := query.Iter()
	defer iter.Close()

	var results []*fraudv1.ScoredTransaction
	var (
		transactionID, cardID  string
		amount                 float64
		merchant, location, cy string
		txTime                 time.Time
	)

	for iter.Scan(&transactionID, &cardID, &amount, &merchant, &location, &txTime, &cy) {
		results = append(results, &fraudv1.ScoredTransaction{
			TransactionId:   transactionID,
			UserId:          userID,
			CardId:          cardID,
			Amount:          amount,
			Currency:        cy,
			Merchant:        merchant,
			Location:        location,
			TransactionTime: timestamppb.New(txTime),
		})
	}

	if err := iter.Close(); err != nil {
		log.Printf("query error in GetUserFraudScoreHistory: %v", err)
		return nil, status.Error(codes.Internal, "failed to query user history")
	}

	return &fraudv1.GetUserFraudScoreHistoryResponse{Transactions: results}, nil
}

func splitReasons(reasons string) []string {
	if reasons == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i < len(reasons); i++ {
		if reasons[i] == ',' {
			result = append(result, reasons[start:i])
			start = i + 1
		}
	}
	result = append(result, reasons[start:])
	return result
}

func main() {
	cassandraHost := getEnv("CASSANDRA_HOST", "cassandra.cassandra.svc.cluster.local")
	cassandraKeyspace := getEnv("CASSANDRA_KEYSPACE", "fraud_detection")
	cassandraUsername := getEnv("CASSANDRA_USERNAME", "cassandra")
	cassandraPassword := getEnv("CASSANDRA_PASSWORD", "cassandra")
	grpcPort := getEnv("GRPC_PORT", "50052")

	cluster := gocql.NewCluster(cassandraHost)
	cluster.Keyspace = cassandraKeyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cassandraUsername,
		Password: cassandraPassword,
	}
	cluster.Consistency = gocql.One
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to connect to Cassandra: %v", err)
	}
	defer session.Close()

	log.Printf("Connected to Cassandra at %s (keyspace=%s)", cassandraHost, cassandraKeyspace)

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	fraudv1.RegisterQueryServiceServer(grpcServer, &server{session: session})

	log.Printf("Query Service listening on :%s", grpcPort)
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
