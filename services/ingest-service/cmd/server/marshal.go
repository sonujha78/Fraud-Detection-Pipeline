package main

import (
	"encoding/json"
	"time"

	fraudv1 "github.com/frauddetection/ingest-service/proto/v1"
)

// transactionJSON mirrors the JSON shape expected by the Kafka Streams
// fraud-detection application (see stream-processor's Transaction.java).
type transactionJSON struct {
	TransactionID   string  `json:"transactionId"`
	UserID          string  `json:"userId"`
	CardID          string  `json:"cardId"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	Merchant        string  `json:"merchant"`
	Location        string  `json:"location"`
	TransactionTime string  `json:"transactionTime"`
}

func marshalTransactionJSON(tx *fraudv1.Transaction) ([]byte, error) {
	ts := tx.GetTransactionTime()
	var timeStr string
	if ts != nil {
		timeStr = ts.AsTime().UTC().Format(time.RFC3339)
	} else {
		timeStr = time.Now().UTC().Format(time.RFC3339)
	}

	payload := transactionJSON{
		TransactionID:   tx.GetTransactionId(),
		UserID:          tx.GetUserId(),
		CardID:          tx.GetCardId(),
		Amount:          tx.GetAmount(),
		Currency:        tx.GetCurrency(),
		Merchant:        tx.GetMerchant(),
		Location:        tx.GetLocation(),
		TransactionTime: timeStr,
	}

	return json.Marshal(payload)
}
