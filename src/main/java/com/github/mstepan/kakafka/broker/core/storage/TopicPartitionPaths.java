package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.broker.utils.Preconditions;
import com.github.mstepan.kakafka.io.IOUtils;
import java.nio.file.Path;

public record TopicPartitionPaths(Path logFilePath, Path indexFilePath) {

    public TopicPartitionPaths {
        Preconditions.checkArgument(logFilePath != null, "null 'logFilePath' detected");
        Preconditions.checkArgument(indexFilePath != null, "null 'indexFilePath' detected");
    }

    public boolean isBothFilesExist() {
        return isLogFileExist() && isIndexFileExist();
    }

    public boolean isLogFileExist() {
        return IOUtils.exist(logFilePath);
    }

    public boolean isIndexFileExist() {
        return IOUtils.exist(indexFilePath);
    }
}
