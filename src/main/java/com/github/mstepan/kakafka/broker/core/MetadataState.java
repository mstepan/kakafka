package com.github.mstepan.kakafka.broker.core;

import java.util.List;

public record MetadataState(String leaderBrokerName, List<LiveBroker> brokers) {

    public String leaderUrl() {
        for (LiveBroker broker : brokers) {
            if (leaderBrokerName.equals(broker.id())) {
                return broker.url();
            }
        }
        throw new IllegalStateException("Can't find leader broker in a list of active brokers");
    }
}
