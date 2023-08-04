package com.github.mstepan.kakafka.broker.core.storage;

import java.nio.file.Path;
import java.util.Objects;

public record TopicPartitionFS(Path logFilePath, Path indexFilePath) {

    public TopicPartitionFS {
        Objects.requireNonNull(logFilePath, "null 'logFilePath' detected");
        Objects.requireNonNull(indexFilePath, "null 'indexFilePath' detected");
    }
}
