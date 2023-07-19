package com.github.mstepan.kakafka.broker.core;

import java.util.Collection;

public record MetadataState(String leaderBrokerName, Collection<LiveBroker> brokers) {

    public LiveBroker leaderBroker() {
        for (LiveBroker broker : brokers) {
            if (leaderBrokerName.equals(broker.id())) {
                return broker;
            }
        }
        throw new IllegalStateException(
                "Can't find leader broker id '%s' in a list of active brokers: %s"
                        .formatted(leaderBrokerName, brokers));
    }

    public String asStr() {
        StringBuilder res = new StringBuilder();
        res.append(
                "leader name:%s, url: %s %n"
                        .formatted(
                                leaderBrokerName,
                                "%s:%d".formatted(leaderBroker().host(), leaderBroker().port())));

        for (LiveBroker broker : brokers) {
            res.append("id: %s, url: %s %n".formatted(broker.id(), broker.url()));
        }

        return res.toString();
    }
}
