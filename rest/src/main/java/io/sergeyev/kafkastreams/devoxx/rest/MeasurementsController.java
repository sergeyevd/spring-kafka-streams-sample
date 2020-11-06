package io.sergeyev.kafkastreams.devoxx.rest;

import io.sergeyev.kafkastreams.common.WeatherMeasurement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/v1/weather")
@Slf4j
public class MeasurementsController {

    private final MeasurementsSender measurementsSender;

    public MeasurementsController(MeasurementsSender measurementsSender) {
        this.measurementsSender = measurementsSender;
    }

    @PostMapping
    public void collectMeasurements(@Valid @RequestBody WeatherMeasurement measurement) {
//        log.info("Got [{}]", measurement);
        measurementsSender.send(measurement);
    }
}
