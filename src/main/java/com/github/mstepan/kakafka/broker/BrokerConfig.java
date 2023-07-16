package com.github.mstepan.kakafka.broker;

public record BrokerConfig(String brokerName, int port) {

    public String url() {
        return "localhost:%d".formatted(port());
    }
}
