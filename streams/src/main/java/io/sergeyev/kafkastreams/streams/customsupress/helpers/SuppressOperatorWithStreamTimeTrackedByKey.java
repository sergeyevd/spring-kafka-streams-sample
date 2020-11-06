package io.sergeyev.kafkastreams.streams.customsupress.helpers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;

@Slf4j
public class SuppressOperatorWithStreamTimeTrackedByKey {

    public static <T> TransformerSupplier<Windowed<Long>, T, Iterable<KeyValue<Long, AggregationSuppressionStateHolder<T>>>> build(String stateStoreName) {
        return () -> new Transformer<>() {
            KeyValueStore<Long, AggregationSuppressionStateHolder<T>> store;

            @SuppressWarnings("unchecked")
            @Override
            public void init(ProcessorContext context) {
                store = (KeyValueStore<Long, AggregationSuppressionStateHolder<T>>) context.getStateStore(stateStoreName);
            }

            @Override
            public Iterable<KeyValue<Long, AggregationSuppressionStateHolder<T>>> transform(Windowed<Long> windowKey, T currentAggregation) {
                Long enrollmentId = windowKey.key();
                AggregationSuppressionStateHolder<T> storedValue = store.get(enrollmentId);

                if (storedValue == null) {
                    log.debug("First-time store openning for Windowed [{}], agg: [{}]", windowKey, currentAggregation);
                    updateStoreWithLatestValue(windowKey, currentAggregation, enrollmentId);
                    return Collections.emptyList();
                } else if(windowKey.window().end() > storedValue.getWindow().getEndTime()) {
                    log.debug("Closing store for Windowed [{}], agg: [{}], stored: [{}]", windowKey, currentAggregation, storedValue);
                    updateStoreWithLatestValue(windowKey, currentAggregation, enrollmentId);
                    return Collections.singletonList(KeyValue.pair(storedValue.getWindow().getKey(), storedValue));
                } else if(storedValue.getWindow().overlap(windowKey.window())) {
                    log.debug("Updating store for Windowed [{}], agg: [{}], stored: [{}]", windowKey, currentAggregation, storedValue);
                    updateStoreWithLatestValue(windowKey, currentAggregation, enrollmentId);
                    return Collections.emptyList();
                }

                log.debug("Skipping record for an expired window. Windowed [{}], agg: [{}], stored: [{}]", windowKey, currentAggregation, storedValue);
                return Collections.emptyList();
            }

            private void updateStoreWithLatestValue(Windowed<Long> windowKey, T currentAggregation, Long enrollmentId) {
                store.put(enrollmentId, new AggregationSuppressionStateHolder<>(currentAggregation, new WindowInfo(windowKey)));
            }

            @Override
            public void close() {
            }
        };
    }
}
