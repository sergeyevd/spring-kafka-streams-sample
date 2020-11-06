package io.sergeyev.kafkastreams.streams;

import io.sergeyev.kafkastreams.common.WeatherTimestamped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WeatherTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        final Object value = record.value();
        if(value instanceof WeatherTimestamped) {
            return ((WeatherTimestamped) value).getEpochTs();
        } else {
            return System.currentTimeMillis();
        }
    }
}
