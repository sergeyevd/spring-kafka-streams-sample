package io.sergeyev.kafkastreams.streams.deduplication;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class DeduplicationProcessorConfig {

    private final static long DEDUPLICATION_WINDOW_LENGTH_2_WEEKS_IN_MS = 2 * 7 * 24 * 60 * 60  * 1000;
    private final static String DEDUPLICATION_WINDOW_STATE_STORE_NAME = "ad-facts-state-store";

//    @Bean
    public Function<KStream<String, String>, KStream<String, String>> adFactsDeduplicator() {
        return input ->
                input.transform(() -> new DeduplicationTransformer<>(TimeUnit.HOURS.toMillis(2), (k, v) -> k), DEDUPLICATION_WINDOW_STATE_STORE_NAME)
                        .map((k, v) -> {
                            log.info("Got K: {}, V: {}", k, v);
                            return KeyValue.pair(k,v);
                        });
    }

    @Bean
    public StoreBuilder<WindowStore<String, Void>> adFactsStateStore() {
        return Stores.windowStoreBuilder(
                Stores.persistentWindowStore(DEDUPLICATION_WINDOW_STATE_STORE_NAME, Duration.ofHours(3), Duration.ofHours(2), false),
                Serdes.String(),
                Serdes.Void()
        );
    }


    /**
     * Discards duplicate records from the input stream.
     * <p>
     * Duplicate records are detected based on an event ID;  in this simplified example, the record
     * value is the event ID.  The transformer remembers known event IDs in an associated window state
     * store, which automatically purges/expires event IDs from the store after a certain amount of
     * time has passed to prevent the store from growing indefinitely.
     * <p>
     * Note: This code is for demonstration purposes and was not tested for production usage.
     */
    private static class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

        private ProcessorContext context;

        /**
         * Key: event ID
         * Value: timestamp (event-time) of the corresponding event when the event ID was seen for the
         * first time
         */
        private WindowStore<E, Void> eventIdStore;

        private final long leftDurationMs;
        private final long rightDurationMs;

        private final KeyValueMapper<K, V, E> idExtractor;

        /**
         * @param maintainDurationPerEventInMs how long to "remember" a known event (or rather, an event
         *                                     ID), during the time of which any incoming duplicates of
         *                                     the event will be dropped, thereby de-duplicating the
         *                                     input.
         * @param idExtractor                  extracts a unique identifier from a record by which we de-duplicate input
         *                                     records; if it returns null, the record will not be considered for
         *                                     de-duping but forwarded as-is.
         */
        DeduplicationTransformer(final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
            if (maintainDurationPerEventInMs < 1) {
                throw new IllegalArgumentException("maintain duration per event must be >= 1");
            }
            leftDurationMs = maintainDurationPerEventInMs / 2;
            rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
            this.idExtractor = idExtractor;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            this.context = context;
            eventIdStore = (WindowStore<E, Void>) context.getStateStore(DEDUPLICATION_WINDOW_STATE_STORE_NAME);
        }

        public KeyValue<K, V> transform(final K key, final V value) {
            final E eventId = idExtractor.apply(key, value);
            if (eventId == null) {
                return KeyValue.pair(key, value);
            } else {
                final KeyValue<K, V> output;
                if (isDuplicate(eventId)) {
                    output = null;
                    updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
                } else {
                    output = KeyValue.pair(key, value);
                    rememberNewEvent(eventId, context.timestamp());
                }
                return output;
            }
        }

        private boolean isDuplicate(final E eventId) {
            final long eventTime = context.timestamp();
            final WindowStoreIterator<Void> timeIterator = eventIdStore.fetch(
                    eventId,
                    eventTime - leftDurationMs,
                    eventTime + rightDurationMs);
            final boolean isDuplicate = timeIterator.hasNext();
            timeIterator.close();
            return isDuplicate;
        }

        private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, final long newTimestamp) {
            eventIdStore.put(eventId, null, newTimestamp);
        }

        private void rememberNewEvent(final E eventId, final long timestamp) {
            eventIdStore.put(eventId, null, timestamp);
        }

        @Override
        public void close() {
            // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
            // The Kafka Streams API will automatically close stores when necessary.
        }

    }

}
