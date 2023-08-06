package com.github.mstepan.kakafka.broker.core;

import com.github.mstepan.kakafka.broker.utils.Preconditions;
import java.util.Collection;
import java.util.Optional;

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

    public Optional<LiveBroker> findBrokerById(String brokerIdToFind) {
        Preconditions.checkNotNull(brokerIdToFind, "null 'brokerIdToFind' parameter passed");
        for (LiveBroker broker : brokers) {
            if (brokerIdToFind.equals(broker.id())) {
                return Optional.of(broker);
            }
        }
        return Optional.empty();
    }
}
