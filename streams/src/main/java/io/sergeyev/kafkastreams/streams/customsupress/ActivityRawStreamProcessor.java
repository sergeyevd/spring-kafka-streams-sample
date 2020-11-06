package io.sergeyev.kafkastreams.streams.customsupress;

import io.sergeyev.kafkastreams.streams.customsupress.helpers.AggregationSuppressionStateHolder;
import io.sergeyev.kafkastreams.streams.customsupress.helpers.SuppressOperatorWithStreamTimeTrackedByKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.function.Function;

import static io.sergeyev.kafkastreams.streams.customsupress.helpers.WindowAggStateStoreType.ACTIVITY;


@Slf4j
@Configuration
public class ActivityRawStreamProcessor {

//    protected final Duration WINDOW_RETENTION_PERIOD;
//    private final Duration WIN_SIZE;
//    private final Duration GRACE;
//    private final String SUPPRESS_STORE_NAME = "activity-suppress-store";
//
//    @Bean
//    public StoreBuilder suppressStoreActivity() {
//        return Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore(SUPPRESS_STORE_NAME), Serdes.Long(), new ActivityAggregateSuppressStoreSerde());
//    }
//
////    @Bean
//    public Function<KStream<Long, ActivityStreamItem>, KStream<Long, ActivityAggResult>> processActivityRaw() {
//        return input -> {
//            KGroupedStream<Long, ActivityStreamItem> grouped = input.groupByKey();
//
//            TimeWindowedKStream<Long, ActivityStreamItem> windowed = grouped.windowedBy(
//                    TimeWindows.of(WIN_SIZE).advanceBy(WIN_SIZE).grace(WINDOW_RETENTION_PERIOD));
//
//            return
//                    windowed.aggregate(makeInitializer(), makeAggregator(),
//                            Materialized.<Long, ActivityAggregate, WindowStore<Bytes, byte[]>>as(ACTIVITY.getStateStoreName())
//                                    .withRetention(WINDOW_RETENTION_PERIOD.plus(WIN_SIZE))
//                                    .withValueSerde(new ActivityAggregateSerde()))
//                            .toStream()
//                            .flatTransform(SuppressOperatorWithStreamTimeTrackedByKey.build(SUPPRESS_STORE_NAME), SUPPRESS_STORE_NAME)
//                            .map(lastInWindow());
//        };
//    }
//
//    private KeyValueMapper<Long, AggregationSuppressionStateHolder<ActivityAggregate>, KeyValue<Long, ActivityAggResult>> lastInWindow() {
//        return (enrollmentId, aggregationHolder) -> {
//            var win = aggregationHolder.getWindow();
//            var agg = aggregationHolder.getAggregationResult();
//            return KeyValue.pair(enrollmentId, new ActivityAggResult(
//                    enrollmentId,
//                    win.getStartTime(),
//                    win.getEndTime(),
//                    agg.getNumberOfActivityRecords(),
//                    agg.getTimeLastStepsValueInWindow(),
//                    agg.getLastStepsValueInWindow(),
//                    agg.getLastEnergyBurnedValueInWindow(),
//                    agg.getDataSourceType()));
//        };
//    }
//
//    private Initializer<ActivityAggregate> makeInitializer() {
//        return () -> new ActivityAggregate(null, 0, 0L, 0, 0);
//    }
//
//    private Aggregator<Long, ActivityStreamItem, ActivityAggregate> makeAggregator() {
//        return (s, activity, agg) -> {
//            boolean hasActivity = activity.getSteps() != null;
//            return new ActivityAggregate(
//                    activity.getDataSourceType(),
//                    hasActivity ? agg.getNumberOfActivityRecords() + 1 : agg.getNumberOfActivityRecords(),
//                    hasActivity ? activity.getTime() : agg.getTimeLastStepsValueInWindow(),
//                    hasActivity ? activity.getSteps() : agg.getLastStepsValueInWindow(),
//                    hasActivity ? activity.getEnergyBurned() : agg.getLastEnergyBurnedValueInWindow());
//        };
//    }
}
