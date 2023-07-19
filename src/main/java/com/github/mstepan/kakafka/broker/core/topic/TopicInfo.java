package com.github.mstepan.kakafka.broker.core.topic;

import java.util.List;

public record TopicInfo(List<TopicPartitionInfo> partitions) {}
