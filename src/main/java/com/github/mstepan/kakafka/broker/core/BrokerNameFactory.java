package com.github.mstepan.kakafka.broker.core;

import java.util.UUID;

public final class BrokerNameFactory {

    public String generateBrokerName() {
        return "broker-%s".formatted(UUID.randomUUID());
    }
}
