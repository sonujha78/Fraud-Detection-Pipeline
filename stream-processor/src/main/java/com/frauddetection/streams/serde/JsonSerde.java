package com.frauddetection.streams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * Generic JSON Serde backed by Jackson, used for all custom types
 * (Transaction, ScoredTransaction, window-state objects) flowing
 * through the Kafka Streams topology and its state stores.
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing value for topic " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), targetType);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing value for topic " + topic, e);
            }
        };
    }
}
