package com.github.mstepan.kakafka.broker.core;

public class MetadataStorage {

    private String leaderBrokerName;

    public synchronized void setLeader(String brokerName) {
        this.leaderBrokerName = brokerName;
    }

    public synchronized String getLeaderBrokerName() {
        return leaderBrokerName;
    }

    public synchronized String getMetadataSnapshot() {
        return """
            {
                "leader": "%s"
                "brokers":
                    {
                    }
            }
            """
                .formatted(leaderBrokerName);
    }
}
