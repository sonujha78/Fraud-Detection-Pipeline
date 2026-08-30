package com.frauddetection.streams.rules;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

/**
 * Centralized fraud-detection thresholds and rule evaluation helpers.
 * Kept separate from the Streams topology so the rules themselves
 * are easy to read, test, and tune independently of the plumbing.
 */
public class FraudRules {

    // Rule 1: max allowed transactions per card within the 5-minute window
    public static final int MAX_TX_PER_CARD_5MIN = 5;

    // Rule 2: an amount more than this many times the user's historical
    // average within the 1-hour window is considered a big deviation
    public static final double AMOUNT_DEVIATION_MULTIPLIER = 5.0;

    // Rule 2 (cold start): if a user has no history yet, any single
    // transaction above this absolute amount is treated as suspicious
    public static final BigDecimal COLD_START_ABSOLUTE_LIMIT = new BigDecimal("50000");

    // Rule 3: minimum time between two transactions from *different*
    // locations on the same card to be considered physically plausible
    public static final Duration IMPOSSIBLE_TRAVEL_WINDOW = Duration.ofMinutes(30);

    // A transaction is flagged (routed to transactions.flagged) once its
    // cumulative risk score reaches this value
    public static final double FLAG_THRESHOLD = 50.0;

    private FraudRules() {
        // static helpers only
    }

    /** Rule 1: velocity check - too many transactions on one card too fast. */
    public static boolean isVelocityBreach(long countInWindow) {
        return countInWindow > MAX_TX_PER_CARD_5MIN;
    }

    /** Rule 2: amount deviates sharply from the user's historical average. */
    public static boolean isAmountDeviation(BigDecimal amount, BigDecimal historicalAverage, long historicalTxCount) {
        if (historicalTxCount == 0) {
            // no history yet - fall back to an absolute cold-start limit
            return amount.compareTo(COLD_START_ABSOLUTE_LIMIT) > 0;
        }
        if (historicalAverage.compareTo(BigDecimal.ZERO) <= 0) {
            return false;
        }
        BigDecimal deviationLimit = historicalAverage.multiply(BigDecimal.valueOf(AMOUNT_DEVIATION_MULTIPLIER));
        return amount.compareTo(deviationLimit) > 0;
    }

    /**
     * Rule 3: impossible travel - same card used in two different locations
     * closer together in time than physically plausible travel would allow.
     */
    public static boolean isImpossibleTravel(String previousLocation, Instant previousTime,
                                              String currentLocation, Instant currentTime) {
        if (previousLocation == null || previousTime == null) {
            return false; // no prior location on record yet
        }
        if (previousLocation.equals(currentLocation)) {
            return false; // same location, no travel involved
        }
        Duration gap = Duration.between(previousTime, currentTime);
        return !gap.isNegative() && gap.compareTo(IMPOSSIBLE_TRAVEL_WINDOW) < 0;
    }

    /** Combines individual rule weights into a single 0-100 risk score. */
    public static double computeRiskScore(boolean velocityBreach, boolean amountDeviation, boolean impossibleTravel) {
        double score = 0.0;
        if (velocityBreach) score += 40.0;
        if (amountDeviation) score += 35.0;
        if (impossibleTravel) score += 45.0;
        return Math.min(score, 100.0);
    }

    public static boolean shouldFlag(double riskScore) {
        return riskScore >= FLAG_THRESHOLD;
    }
}
