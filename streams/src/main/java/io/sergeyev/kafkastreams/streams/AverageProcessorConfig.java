package io.sergeyev.kafkastreams.streams;

import io.sergeyev.kafkastreams.common.WeatherMeasurement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.function.Function;

@Configuration
public class AverageProcessorConfig {

    @Bean
    public Function<KStream<Long, WeatherMeasurement>, KStream<Long, WeatherMeasurementAggregation>> averageProcessor() {
        return input -> input
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ZERO))
                .aggregate(this::init, this::agg, Materialized.with(Serdes.Long(), new JsonSerde<>(IntermediateAggState.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map(this::avg);
    }

    private KeyValue<Long, WeatherMeasurementAggregation> avg(Windowed<Long> window, IntermediateAggState aggState) {
        return KeyValue.pair(window.key(), new WeatherMeasurementAggregation(window.key(), aggState.getTempSum() / aggState.getTempCount(), aggState.getHumSum() / aggState.getTempCount(), window.window().endTime()));
    }

    private IntermediateAggState agg(Long deviceId, WeatherMeasurement incoming, IntermediateAggState aggState) {
        return new IntermediateAggState(
                    aggState.getTempCount() + 1,
                    aggState.getTempSum() + incoming.getTemperatureCelsius().doubleValue(),
                    aggState.getHumCount() + 1,
                    aggState.getHumSum() + incoming.getHumidity().doubleValue()
        );
    }

    private IntermediateAggState init() {
        return new IntermediateAggState();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class IntermediateAggState {
        private long tempCount;
        private double tempSum;
        private long humCount;
        private double humSum;
    }
}
