package io.sergeyev.kafkastreams.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class AlertsProcessorConfig {

    @Bean
    public Consumer<KStream<Long, WeatherMeasurementAggregation>> alertsProcessor() {
        return input -> input
                .foreach(this::thresholdAlert);
    }

    private void thresholdAlert(Long deviceId, WeatherMeasurementAggregation weatherMeasurementAggregation) {
        final Double humAvg = weatherMeasurementAggregation.getHumAvg();
        if(humAvg > 42d) {
            log.warn("Threshold has been reached for device [{}], value: [{}]", deviceId, humAvg);
        }
    }
}
