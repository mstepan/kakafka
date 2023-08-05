package com.github.mstepan.kakafka.broker.core.storage;

import java.nio.file.Path;
import java.util.Objects;

public record TopicPartitionPaths(Path logFilePath, Path indexFilePath) {

    public TopicPartitionPaths {
        Objects.requireNonNull(logFilePath, "null 'logFilePath' detected");
        Objects.requireNonNull(indexFilePath, "null 'indexFilePath' detected");
    }
}
