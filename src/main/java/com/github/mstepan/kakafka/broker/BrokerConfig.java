package com.github.mstepan.kakafka.broker;

public record BrokerConfig(String brokerName, int port, String etcdEndpoint, String dataFolder) {

    public static final String BROKER_KEY_PREFIX = "/kakafka/brokers";

    public String url() {
        return "localhost:%d".formatted(port());
    }
}
