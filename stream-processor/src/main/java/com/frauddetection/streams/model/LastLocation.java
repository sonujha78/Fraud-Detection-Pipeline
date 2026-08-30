package com.frauddetection.streams.model;

import java.time.Instant;

/**
 * Tracks the most recent transaction location and time for a card,
 * used for the impossible-travel fraud check.
 */
public class LastLocation {

    private String cardId;
    private String location;
    private Instant seenAt;

    public LastLocation() {}

    public LastLocation(String cardId, String location, Instant seenAt) {
        this.cardId = cardId;
        this.location = location;
        this.seenAt = seenAt;
    }

    public String getCardId() { return cardId; }
    public void setCardId(String cardId) { this.cardId = cardId; }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    public Instant getSeenAt() { return seenAt; }
    public void setSeenAt(Instant seenAt) { this.seenAt = seenAt; }
}
