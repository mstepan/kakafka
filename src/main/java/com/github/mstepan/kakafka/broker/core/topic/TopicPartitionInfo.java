package com.github.mstepan.kakafka.broker.core.topic;

import com.github.mstepan.kakafka.broker.utils.ValidationUtils;
import java.util.List;
import java.util.Objects;

public record TopicPartitionInfo(int idx, String leader, List<String> replicas) {

    public TopicPartitionInfo {
        ValidationUtils.checkArgument(idx >= 0, "negative 'idx' value detected: %d".formatted(idx));
        Objects.requireNonNull(leader, "null 'leader' detected");
        Objects.requireNonNull(replicas, "null 'replicas' detected");
    }
}
