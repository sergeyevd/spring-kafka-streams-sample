package io.sergeyev.kafkastreams.streams.customsupress.helpers;

public enum WindowAggStateStoreType {

    ACTIVITY {
        @Override
        public String getStateStoreName() {
            return "activity_agg_table6";
        }
    },

    BP {
        @Override
        public String getStateStoreName() {
            return "bp_agg_table6";
        }
    },

    HEART_RATE {
        @Override
        public String getStateStoreName() {
            return "hr_agg_table6";
        }
    },

    OXY {
        @Override
        public String getStateStoreName() {
            return "oxy_agg_table6";
        }
    },

    TEMPERATURE {
        @Override
        public String getStateStoreName() {
            return "temp_agg_table6";
        }
    };

    public abstract String getStateStoreName();
}
