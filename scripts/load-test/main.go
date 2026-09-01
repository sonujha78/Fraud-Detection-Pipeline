package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type Transaction struct {
	TransactionID   string  `json:"transactionId"`
	UserID          string  `json:"userId"`
	CardID          string  `json:"cardId"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	Merchant        string  `json:"merchant"`
	Location        string  `json:"location"`
	TransactionTime string  `json:"transactionTime"`
}

func main() {
	targetTxPerMin := 800   // conservative target given local resource constraints
	durationSeconds := 15   // burst duration
	numWorkers := 5

	totalTx := (targetTxPerMin * durationSeconds) / 60
	intervalPerWorker := time.Duration(float64(durationSeconds) * float64(time.Second) / float64(totalTx) * float64(numWorkers))

	writer := &kafka.Writer{
		Addr:                   kafka.TCP("kafka.kafka.svc.cluster.local:9092"),
		Topic:                  "transactions.raw",
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              10,
		BatchTimeout:           50 * time.Millisecond,
	}
	defer writer.Close()

	var sent int64
	var failed int64
	locations := []string{"IN-DEL", "IN-MUM", "IN-BLR", "US-NYC", "UK-LON"}
	merchants := []string{"Amazon", "Flipkart", "Swiggy", "Uber", "Netflix"}

	var wg sync.WaitGroup
	startTime := time.Now()
	perWorker := totalTx / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				cardID := fmt.Sprintf("load-card-%d-%d", workerID, i%20)
				tx := Transaction{
					TransactionID:   fmt.Sprintf("load-tx-%d-%d-%d", workerID, i, time.Now().UnixNano()),
					UserID:          fmt.Sprintf("load-user-%d-%d", workerID, i%20),
					CardID:          cardID,
					Amount:          float64(rand.Intn(9500) + 100),
					Currency:        "INR",
					Merchant:        merchants[rand.Intn(len(merchants))],
					Location:        locations[rand.Intn(len(locations))],
					TransactionTime: time.Now().UTC().Format(time.RFC3339),
				}
				payload, _ := json.Marshal(tx)

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				err := writer.WriteMessages(ctx, kafka.Message{
					Key:   []byte(cardID),
					Value: payload,
				})
				cancel()

				if err != nil {
					atomic.AddInt64(&failed, 1)
				} else {
					atomic.AddInt64(&sent, 1)
				}

				time.Sleep(intervalPerWorker)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	fmt.Println("=== Load Test Results ===")
	fmt.Printf("Duration: %s\n", elapsed)
	fmt.Printf("Target: %d tx/min for %ds (%d total)\n", targetTxPerMin, durationSeconds, totalTx)
	fmt.Printf("Sent successfully: %d\n", atomic.LoadInt64(&sent))
	fmt.Printf("Failed: %d\n", atomic.LoadInt64(&failed))
	fmt.Printf("Effective rate: %.1f tx/sec\n", float64(sent)/elapsed.Seconds())

	if failed > 0 {
		log.Printf("WARNING: %d messages failed to send", failed)
	}
}
