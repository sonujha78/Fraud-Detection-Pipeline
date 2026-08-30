package com.frauddetection.streams.model;

/**
 * Aggregated state for the 5-minute per-card transaction count window.
 */
public class CardWindowCount {

    private String cardId;
    private long count;

    public CardWindowCount() {}

    public CardWindowCount(String cardId, long count) {
        this.cardId = cardId;
        this.count = count;
    }

    public static CardWindowCount initial(String cardId) {
        return new CardWindowCount(cardId, 0);
    }

    public CardWindowCount increment() {
        this.count += 1;
        return this;
    }

    public String getCardId() { return cardId; }
    public void setCardId(String cardId) { this.cardId = cardId; }
    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }
}
