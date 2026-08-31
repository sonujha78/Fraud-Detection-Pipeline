package com.frauddetection.streams;

import com.frauddetection.streams.model.*;
import com.frauddetection.streams.rules.FraudRules;
import com.frauddetection.streams.serde.JsonSerde;
import com.frauddetection.streams.storage.CassandraWriter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Real-time fraud-detection stream processing topology.
 *
 * Reads raw transactions, maintains windowed / keyed state (per-card
 * velocity count, per-user rolling average, per-card last known location),
 * applies fraud rules to every transaction, emits scored results to
 * "transactions.scored" and high-risk results to "transactions.flagged",
 * and persists every scored transaction to Cassandra.
 */
public class FraudDetectionApp {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionApp.class);

    private static final String RAW_TOPIC = "transactions.raw";
    private static final String SCORED_TOPIC = "transactions.scored";
    private static final String FLAGGED_TOPIC = "transactions.flagged";
    private static final String LAST_LOCATION_STORE = "last-location-store";
    private static final String CARD_VELOCITY_STORE = "card-velocity-store";
    private static final String USER_AMOUNT_STORE = "user-amount-store";

    public static void main(String[] args) {
        String bootstrapServers = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092");
        String cassandraHost = System.getenv().getOrDefault("CASSANDRA_HOST", "cassandra.cassandra.svc.cluster.local");
        int cassandraPort = Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_PORT", "9042"));
        String cassandraDatacenter = System.getenv().getOrDefault("CASSANDRA_DATACENTER", "datacenter1");
        String cassandraKeyspace = System.getenv().getOrDefault("CASSANDRA_KEYSPACE", "fraud_detection");
        String cassandraUsername = System.getenv().getOrDefault("CASSANDRA_USERNAME", "cassandra");
        String cassandraPassword = System.getenv().getOrDefault("CASSANDRA_PASSWORD", "bAxVShxT1B");

        CassandraWriter cassandraWriter = new CassandraWriter(cassandraHost, cassandraPort, cassandraDatacenter, cassandraKeyspace, cassandraUsername, cassandraPassword);

        Properties props = buildStreamsConfig(bootstrapServers);
        Topology topology = buildTopology(cassandraWriter);

        log.info("Starting Fraud Detection Streams app. Bootstrap servers: {}", bootstrapServers);
        log.info("Topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Fraud Detection Streams app...");
            streams.close(Duration.ofSeconds(10));
            cassandraWriter.close();
        }));

        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception in Kafka Streams, replacing thread", exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start();
    }

    static Properties buildStreamsConfig(String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return props;
    }

    static Topology buildTopology(CassandraWriter cassandraWriter) {
        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<Transaction> txSerde = new JsonSerde<>(Transaction.class);
        JsonSerde<ScoredTransaction> scoredSerde = new JsonSerde<>(ScoredTransaction.class);
        JsonSerde<LastLocation> lastLocSerde = new JsonSerde<>(LastLocation.class);
        JsonSerde<UserAmountWindow> userAmtSerde = new JsonSerde<>(UserAmountWindow.class);

        StoreBuilder<KeyValueStore<String, LastLocation>> lastLocationStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(LAST_LOCATION_STORE),
                        Serdes.String(),
                        lastLocSerde);
        builder.addStateStore(lastLocationStoreBuilder);

        // Velocity and amount-deviation state is now maintained directly inside the
        // TransactionScorer (see below) using these window stores, instead of via a
        // separate re-keyed sub-topology. This avoids the repartition round-trip that
        // previously caused a transaction's own contribution to be invisible to itself
        // at scoring time (classic read-your-own-write race in a distributed counting
        // topology).
        StoreBuilder<WindowStore<String, Long>> cardVelocityStoreBuilder =
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(CARD_VELOCITY_STORE, Duration.ofMinutes(10), Duration.ofMinutes(5), false),
                        Serdes.String(),
                        Serdes.Long());
        builder.addStateStore(cardVelocityStoreBuilder);

        StoreBuilder<WindowStore<String, UserAmountWindow>> userAmountStoreBuilder =
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(USER_AMOUNT_STORE, Duration.ofHours(2), Duration.ofHours(1), false),
                        Serdes.String(),
                        userAmtSerde);
        builder.addStateStore(userAmountStoreBuilder);

        KStream<String, Transaction> rawStream = builder.stream(
                RAW_TOPIC, Consumed.with(Serdes.String(), txSerde));

        KStream<String, ScoredTransaction> scored = rawStream.transformValues(
                TransactionScorer::new,
                LAST_LOCATION_STORE, CARD_VELOCITY_STORE, USER_AMOUNT_STORE
        );

        scored.to(SCORED_TOPIC, Produced.with(Serdes.String(), scoredSerde));

        scored.filter((k, v) -> v.isFlagged())
                .to(FLAGGED_TOPIC, Produced.with(Serdes.String(), scoredSerde));

        scored.foreach((k, v) -> cassandraWriter.write(v));

        return builder.build();
    }

    /**
     * Stateful transformer that scores each transaction using three
     * directly-maintained state stores: last-known-location (impossible
     * travel), a 5-minute tumbling window of per-card transaction counts
     * (velocity), and a 1-hour tumbling window of per-user amount sum/count
     * (amount deviation). All three are read AND updated within the same
     * transform() call per record, so a transaction always sees its own
     * contribution - no cross-topology race condition.
     */
    static class TransactionScorer implements ValueTransformer<Transaction, ScoredTransaction> {

        private KeyValueStore<String, LastLocation> lastLocationStore;
        private WindowStore<String, Long> cardVelocityStore;
        private WindowStore<String, UserAmountWindow> userAmountStore;

        private static final long FIVE_MIN_MS = Duration.ofMinutes(5).toMillis();
        private static final long ONE_HOUR_MS = Duration.ofHours(1).toMillis();

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.lastLocationStore = (KeyValueStore<String, LastLocation>) context.getStateStore(LAST_LOCATION_STORE);
            this.cardVelocityStore = (WindowStore<String, Long>) context.getStateStore(CARD_VELOCITY_STORE);
            this.userAmountStore = (WindowStore<String, UserAmountWindow>) context.getStateStore(USER_AMOUNT_STORE);
        }

        @Override
        public ScoredTransaction transform(Transaction tx) {
            List<String> reasons = new ArrayList<>();
            long txTimeMs = tx.getTransactionTime().toEpochMilli();

            // ---- Rule 3: impossible travel ----
            LastLocation last = lastLocationStore.get(tx.getCardId());
            boolean impossibleTravel = false;
            if (last != null) {
                impossibleTravel = FraudRules.isImpossibleTravel(
                        last.getLocation(), last.getSeenAt(),
                        tx.getLocation(), tx.getTransactionTime());
            }
            lastLocationStore.put(tx.getCardId(),
                    new LastLocation(tx.getCardId(), tx.getLocation(), tx.getTransactionTime()));
            if (impossibleTravel) {
                reasons.add("IMPOSSIBLE_TRAVEL");
            }

            // ---- Rule 1: velocity - 5-minute tumbling window count per card ----
            long windowStart = txTimeMs - (txTimeMs % FIVE_MIN_MS);
            Long existingCount = cardVelocityStore.fetch(tx.getCardId(), windowStart);
            long newCount = (existingCount == null ? 0L : existingCount) + 1;
            cardVelocityStore.put(tx.getCardId(), newCount, windowStart);
            boolean velocityBreach = FraudRules.isVelocityBreach(newCount);
            if (velocityBreach) {
                reasons.add("VELOCITY_BREACH");
            }

            // ---- Rule 2: amount deviation - 1-hour tumbling window sum/avg per user ----
            long hourWindowStart = txTimeMs - (txTimeMs % ONE_HOUR_MS);
            UserAmountWindow existing = userAmountStore.fetch(tx.getUserId(), hourWindowStart);
            BigDecimal historicalAverage = existing != null ? existing.average() : BigDecimal.ZERO;
            long historicalTxCount = existing != null ? existing.getTxCount() : 0;
            boolean amountDeviation = FraudRules.isAmountDeviation(tx.getAmount(), historicalAverage, historicalTxCount);
            if (amountDeviation) {
                reasons.add("AMOUNT_DEVIATION");
            }
            UserAmountWindow updated = existing != null ? existing : UserAmountWindow.initial(tx.getUserId());
            updated.add(tx.getAmount());
            userAmountStore.put(tx.getUserId(), updated, hourWindowStart);

            double riskScore = FraudRules.computeRiskScore(velocityBreach, amountDeviation, impossibleTravel);
            boolean flagged = FraudRules.shouldFlag(riskScore);

            if (flagged) {
                log.warn("FLAGGED transaction {} for user {} - reasons={} score={}",
                        tx.getTransactionId(), tx.getUserId(), reasons, riskScore);
            }

            return ScoredTransaction.from(tx, riskScore, flagged, reasons);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
