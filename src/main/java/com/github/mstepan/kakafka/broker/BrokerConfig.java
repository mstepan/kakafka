package com.github.mstepan.kakafka.broker;

public record BrokerConfig(String brokerName, int port, String etcdEndpoint) {

    public String url() {
        return "localhost:%d".formatted(port());
    }
}
