package com.frauddetection.streams.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.frauddetection.streams.model.ScoredTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Writes scored transactions to Cassandra:
 *  - every scored transaction goes to transactions_by_user (historical lookup)
 *  - flagged transactions additionally go to flagged_transactions (ops dashboard)
 *
 * Uses a single shared CqlSession for the lifetime of the Streams app.
 */
public class CassandraWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CassandraWriter.class);

    private final CqlSession session;
    private final PreparedStatement insertByUser;
    private final PreparedStatement insertFlagged;

    public CassandraWriter(String contactHost, int contactPort, String datacenter, String keyspace,
                            String username, String password) {
        var builder = CqlSession.builder()
                .addContactPoint(InetSocketAddress.createUnresolved(contactHost, contactPort))
                .withLocalDatacenter(datacenter)
                .withKeyspace(keyspace);

        if (username != null && !username.isEmpty()) {
            builder.withAuthCredentials(username, password);
        }

        this.session = builder.build();

        this.insertByUser = session.prepare(
                "INSERT INTO transactions_by_user " +
                        "(user_id, transaction_time, transaction_id, card_id, amount, merchant, location, currency) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        this.insertFlagged = session.prepare(
                "INSERT INTO flagged_transactions " +
                        "(flag_date, flagged_time, transaction_id, user_id, card_id, amount, risk_score, risk_reason, location) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        log.info("Connected to Cassandra at {}:{} (datacenter={}, keyspace={})", contactHost, contactPort, datacenter, keyspace);
    }

    public void write(ScoredTransaction tx) {
        try {
            UUID userId = safeUuid(tx.getUserId());
            UUID cardId = safeUuid(tx.getCardId());
            UUID transactionId = safeUuid(tx.getTransactionId());
            Instant txTime = tx.getTransactionTime() != null ? tx.getTransactionTime() : Instant.now();
            BigDecimal amount = tx.getAmount() != null ? tx.getAmount() : BigDecimal.ZERO;

            session.execute(insertByUser.bind(
                    userId, txTime, transactionId, cardId, amount,
                    tx.getMerchant(), tx.getLocation(), tx.getCurrency()));

            if (tx.isFlagged()) {
                LocalDate flagDate = txTime.atZone(ZoneOffset.UTC).toLocalDate();
                String reasons = tx.getRiskReasons() == null ? "" : String.join(",", tx.getRiskReasons());

                session.execute(insertFlagged.bind(
                        flagDate, txTime, transactionId, userId, cardId, amount,
                        tx.getRiskScore(), reasons, tx.getLocation()));
            }
        } catch (Exception e) {
            log.error("Failed to write scored transaction {} to Cassandra: {}", tx.getTransactionId(), e.getMessage());
        }
    }

    private UUID safeUuid(String rawId) {
        if (rawId == null) return new UUID(0, 0);
        try {
            return UUID.fromString(rawId);
        } catch (IllegalArgumentException e) {
            return UUID.nameUUIDFromBytes(rawId.getBytes());
        }
    }

    @Override
    public void close() {
        session.close();
    }
}
