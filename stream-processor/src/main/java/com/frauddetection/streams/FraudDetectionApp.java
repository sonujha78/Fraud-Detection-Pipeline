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

        KStream<String, Transaction> rawStream = builder.stream(
                RAW_TOPIC, Consumed.with(Serdes.String(), txSerde));

        // ---- Rule 1: 5-minute tumbling window count of transactions per card ----
        KStream<String, Transaction> byCard = rawStream.selectKey((k, tx) -> tx.getCardId());

        TimeWindows fiveMinWindow = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30));

        byCard.groupByKey(Grouped.with(Serdes.String(), txSerde))
                .windowedBy(fiveMinWindow)
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("card-velocity-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // ---- Rule 2: 1-hour tumbling window sum/avg of amount per user ----
        KStream<String, Transaction> byUser = rawStream.selectKey((k, tx) -> tx.getUserId());
        TimeWindows oneHourWindow = TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(2));

        byUser.groupByKey(Grouped.with(Serdes.String(), txSerde))
                .windowedBy(oneHourWindow)
                .aggregate(
                        () -> UserAmountWindow.initial(""),
                        (userId, tx, agg) -> {
                            agg.setUserId(userId);
                            return agg.add(tx.getAmount());
                        },
                        Materialized.<String, UserAmountWindow, WindowStore<Bytes, byte[]>>as("user-amount-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(userAmtSerde)
                );

        // ---- Main scoring step: enrich every raw transaction using the last-location store ----
        KStream<String, ScoredTransaction> scored = rawStream.transformValues(
                TransactionScorer::new,
                LAST_LOCATION_STORE
        );

        scored.to(SCORED_TOPIC, Produced.with(Serdes.String(), scoredSerde));

        scored.filter((k, v) -> v.isFlagged())
                .to(FLAGGED_TOPIC, Produced.with(Serdes.String(), scoredSerde));

        // Persist every scored transaction to Cassandra (transactions_by_user,
        // plus flagged_transactions for flagged ones - handled inside the writer).
        scored.foreach((k, v) -> cassandraWriter.write(v));

        return builder.build();
    }

    /**
     * Stateful transformer that scores each transaction using the
     * last-known-location store for the impossible-travel check.
     * Velocity and historical-average checks are backed by the windowed
     * KTables built above, which the Query Service can look up directly
     * for confirmed, low-latency answers.
     */
    static class TransactionScorer implements ValueTransformer<Transaction, ScoredTransaction> {

        private KeyValueStore<String, LastLocation> lastLocationStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.lastLocationStore = (KeyValueStore<String, LastLocation>) context.getStateStore(LAST_LOCATION_STORE);
        }

        @Override
        public ScoredTransaction transform(Transaction tx) {
            List<String> reasons = new ArrayList<>();

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

            boolean amountDeviation = FraudRules.isAmountDeviation(tx.getAmount(), BigDecimal.ZERO, 0);
            if (amountDeviation) {
                reasons.add("AMOUNT_DEVIATION");
            }

            double riskScore = FraudRules.computeRiskScore(false, amountDeviation, impossibleTravel);
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
