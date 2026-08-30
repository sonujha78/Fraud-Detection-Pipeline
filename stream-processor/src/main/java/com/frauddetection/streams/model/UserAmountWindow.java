package com.frauddetection.streams.model;

import java.math.BigDecimal;

/**
 * Aggregated state for the 1-hour per-user amount sum window,
 * used to detect abnormal deviation from a user's typical spend.
 */
public class UserAmountWindow {

    private String userId;
    private BigDecimal sum;
    private long txCount;

    public UserAmountWindow() {}

    public UserAmountWindow(String userId, BigDecimal sum, long txCount) {
        this.userId = userId;
        this.sum = sum;
        this.txCount = txCount;
    }

    public static UserAmountWindow initial(String userId) {
        return new UserAmountWindow(userId, BigDecimal.ZERO, 0);
    }

    public UserAmountWindow add(BigDecimal amount) {
        this.sum = this.sum.add(amount);
        this.txCount += 1;
        return this;
    }

    public BigDecimal average() {
        if (txCount == 0) return BigDecimal.ZERO;
        return sum.divide(BigDecimal.valueOf(txCount), 2, java.math.RoundingMode.HALF_UP);
    }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public BigDecimal getSum() { return sum; }
    public void setSum(BigDecimal sum) { this.sum = sum; }
    public long getTxCount() { return txCount; }
    public void setTxCount(long txCount) { this.txCount = txCount; }
}
