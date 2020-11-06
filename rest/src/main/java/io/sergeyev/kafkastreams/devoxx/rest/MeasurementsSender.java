package io.sergeyev.kafkastreams.devoxx.rest;

import io.sergeyev.kafkastreams.common.WeatherMeasurement;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MeasurementsSender {

    private final KafkaTemplate<Long, WeatherMeasurement> kafkaTemplate;

    public MeasurementsSender(KafkaTemplate<Long, WeatherMeasurement> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(WeatherMeasurement measurement) {
        kafkaTemplate.send("weather-data", measurement.getDeviceId(), measurement);
    }
}
