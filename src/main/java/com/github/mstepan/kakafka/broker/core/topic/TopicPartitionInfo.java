package com.github.mstepan.kakafka.broker.core.topic;

import java.util.List;

public record TopicPartitionInfo(String leader, List<String> replicas) {}
