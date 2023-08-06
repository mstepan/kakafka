package com.github.mstepan.kakafka.broker.core.topic;

import com.github.mstepan.kakafka.broker.utils.Preconditions;
import java.util.List;

public record TopicPartitionInfo(int idx, String leader, List<String> replicas) {

    public TopicPartitionInfo {
        Preconditions.checkArgument(idx >= 0, "negative 'idx' value detected: %d".formatted(idx));
        Preconditions.checkNotNull(leader, "null 'leader' detected");
        Preconditions.checkNotNull(replicas, "null 'replicas' detected");
    }
}
