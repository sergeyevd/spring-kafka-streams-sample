package io.sergeyev.kafkastreams.streams.customsupress;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class ActivityAggregateSerde implements org.apache.kafka.common.serialization.Serde<ActivityAggregate> {
    @Override
    public Serializer<ActivityAggregate> serializer() {
        return null;
    }

    @Override
    public Deserializer<ActivityAggregate> deserializer() {
        return null;
    }
}
