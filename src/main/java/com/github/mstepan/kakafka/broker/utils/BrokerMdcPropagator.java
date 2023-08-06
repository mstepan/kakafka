package com.github.mstepan.kakafka.broker.utils;

import org.slf4j.MDC;

public record BrokerMdcPropagator(String brokerName) implements AutoCloseable {

    private static final String BROKER_NAME_MDC_KEY = "broker.name";

    public BrokerMdcPropagator {
        MDC.put(BROKER_NAME_MDC_KEY, brokerName);
    }

    @Override
    public void close() {
        MDC.remove(BROKER_NAME_MDC_KEY);
    }
}
