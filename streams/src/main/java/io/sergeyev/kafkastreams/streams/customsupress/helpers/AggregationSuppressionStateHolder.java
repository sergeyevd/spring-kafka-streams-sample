package io.sergeyev.kafkastreams.streams.customsupress.helpers;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AggregationSuppressionStateHolder<T> {
    private T aggregationResult;
    private WindowInfo window;
}