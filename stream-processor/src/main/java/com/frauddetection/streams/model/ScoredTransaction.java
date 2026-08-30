package com.frauddetection.streams.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ScoredTransaction {

    private String transactionId;
    private String userId;
    private String cardId;
    private BigDecimal amount;
    private String currency;
    private String merchant;
    private String location;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant transactionTime;

    private double riskScore;
    private boolean flagged;
    private List<String> riskReasons;

    public ScoredTransaction() {}

    public static ScoredTransaction from(Transaction tx, double riskScore, boolean flagged, List<String> riskReasons) {
        ScoredTransaction s = new ScoredTransaction();
        s.transactionId = tx.getTransactionId();
        s.userId = tx.getUserId();
        s.cardId = tx.getCardId();
        s.amount = tx.getAmount();
        s.currency = tx.getCurrency();
        s.merchant = tx.getMerchant();
        s.location = tx.getLocation();
        s.transactionTime = tx.getTransactionTime();
        s.riskScore = riskScore;
        s.flagged = flagged;
        s.riskReasons = riskReasons;
        return s;
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
    public double getRiskScore() { return riskScore; }
    public void setRiskScore(double riskScore) { this.riskScore = riskScore; }
    public boolean isFlagged() { return flagged; }
    public void setFlagged(boolean flagged) { this.flagged = flagged; }
    public List<String> getRiskReasons() { return riskReasons; }
    public void setRiskReasons(List<String> riskReasons) { this.riskReasons = riskReasons; }

    @Override
    public String toString() {
        return "ScoredTransaction{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", riskScore=" + riskScore +
                ", flagged=" + flagged +
                ", riskReasons=" + riskReasons +
                '}';
    }
}
