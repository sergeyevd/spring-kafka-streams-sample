package io.sergeyev.kafkastreams.streams;

import io.sergeyev.kafkastreams.common.WeatherTimestamped;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WeatherMeasurementAggregation implements WeatherTimestamped {
    private Long deviceId;
    private Double tempAvg;
    private Double humAvg;
    private Instant timestamp;

    @Override
    public long getEpochTs() {
        return timestamp.toEpochMilli();
    }
}
