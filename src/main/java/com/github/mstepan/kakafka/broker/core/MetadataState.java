package com.github.mstepan.kakafka.broker.core;

import java.util.List;

public record MetadataState(String leaderBrokerName, List<LiveBroker> brokers) {}
