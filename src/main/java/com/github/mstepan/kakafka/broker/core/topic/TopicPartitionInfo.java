package com.github.mstepan.kakafka.broker.core.topic;

import java.util.List;
import java.util.Objects;

public record TopicPartitionInfo(String leader, List<String> replicas) {

    public TopicPartitionInfo {
        Objects.requireNonNull(leader, "null 'leader' detected");
        Objects.requireNonNull(replicas, "null 'replicas' detected");
    }
}
