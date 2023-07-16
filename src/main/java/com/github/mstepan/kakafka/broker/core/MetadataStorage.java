package com.github.mstepan.kakafka.broker.core;

import java.util.concurrent.CopyOnWriteArraySet;

public class MetadataStorage {

    private String leaderBrokerName;

    private final CopyOnWriteArraySet<LiveBroker> liveBrokers = new CopyOnWriteArraySet<>();

    public synchronized void setLeader(String brokerName) {
        this.leaderBrokerName = brokerName;
    }

    public void addBroker(LiveBroker broker) {
        liveBrokers.add(broker);
    }

    public synchronized String getLeaderBrokerName() {
        return leaderBrokerName;
    }

    public synchronized String getMetadataSnapshot() {

        StringBuilder brokersData = new StringBuilder();
        for (LiveBroker singleBroker : liveBrokers) {
            brokersData.append("{");

            brokersData.append(
                    """
                "id": "%s",
                "url": "%s"
            """
                            .formatted(singleBroker.id(), singleBroker.url()));

            brokersData.append("}");
        }

        return """
            {
                "leader": "%s"
                "brokers": ["%s"]
            }
            """
                .formatted(leaderBrokerName, brokersData);
    }
}
