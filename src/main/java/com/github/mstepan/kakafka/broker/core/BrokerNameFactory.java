package com.github.mstepan.kakafka.broker.core;

import java.util.UUID;

public class BrokerNameFactory {

    public String generateBrokerName() {
        return "broker-%s".formatted(UUID.randomUUID());
    }
}
