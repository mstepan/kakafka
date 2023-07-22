package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.LiveBroker;

public record BrokerHost(String host, int port) {

    public static BrokerHost fromLiveBroker(LiveBroker liveBroker) {
        return new BrokerHost(liveBroker.host(), liveBroker.port());
    }

    @Override
    public String toString() {
        return "%s:%d".formatted(host, port);
    }
}
