package io.sergeyev.kafkastreams.common;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.time.Instant;

@Data
public class WeatherMeasurement implements WeatherTimestamped {
    @NotNull
    private Long deviceId;
    @NotNull
    private Float temperatureCelsius;
    @NotNull
    private Float humidity;
    @NotNull
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;

    @Override
    public long getEpochTs() {
        return timestamp.toEpochMilli();
    }
}
