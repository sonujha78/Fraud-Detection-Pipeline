package com.frauddetection.streams.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction {

    private String transactionId;
    private String userId;
    private String cardId;
    private BigDecimal amount;
    private String currency;
    private String merchant;
    private String location;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant transactionTime;

    public Transaction() {}

    public Transaction(String transactionId, String userId, String cardId, BigDecimal amount,
                        String currency, String merchant, String location, Instant transactionTime) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.cardId = cardId;
        this.amount = amount;
        this.currency = currency;
        this.merchant = merchant;
        this.location = location;
        this.transactionTime = transactionTime;
    }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getCardId() { return cardId; }
    public void setCardId(String cardId) { this.cardId = cardId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public String getMerchant() { return merchant; }
    public void setMerchant(String merchant) { this.merchant = merchant; }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    public Instant getTransactionTime() { return transactionTime; }
    public void setTransactionTime(Instant transactionTime) { this.transactionTime = transactionTime; }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", cardId='" + cardId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", merchant='" + merchant + '\'' +
                ", location='" + location + '\'' +
                ", transactionTime=" + transactionTime +
                '}';
    }
}
