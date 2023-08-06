package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.io.Preconditions;

import java.nio.file.Path;

public record TopicPartitionPaths(Path logFilePath, Path indexFilePath) {

    public TopicPartitionPaths {
        Preconditions.checkArgument(logFilePath != null, "null 'logFilePath' detected");
        Preconditions.checkArgument(indexFilePath != null, "null 'indexFilePath' detected");
    }
}
