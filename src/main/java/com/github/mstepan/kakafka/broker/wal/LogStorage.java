package com.github.mstepan.kakafka.broker.wal;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.storage.PartitionFile;
import com.github.mstepan.kakafka.io.IOUtils;
import com.github.mstepan.kakafka.io.RandomWritableFile;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class LogStorage {

    private final Lock globalStorageLock = new ReentrantLock();

    private final BrokerConfig config;

    private final Path brokerDataFolder;

    private final Map<String, PartitionFile> topicAndPartitionToFile = new HashMap<>();

    public LogStorage(BrokerConfig config) {
        this.config = Objects.requireNonNull(config, "null 'config' detected");
        this.brokerDataFolder = Path.of(config.dataFolder(), config.brokerName());
    }

    public void init() {
        IOUtils.createFolderIfNotExist(config.brokerName(), brokerDataFolder);
    }

    public void appendMessage(String topicName, int partitionIdx, StringTopicMessage msg) {
        globalStorageLock.lock();
        try {

            PartitionFile partitionFile = getPartitionFile(topicName, partitionIdx);

            RandomWritableFile writableLogFile = partitionFile.log();

            long msgOffset = writableLogFile.end();
            writableLogFile.append(msg.key(), msg.value());

            long msgIdx = 0; // todo: read last message index from index file if not empty
            RandomWritableFile writableIndexFile = partitionFile.index();
            writableIndexFile.append(msgIdx, msgOffset);
        } finally {
            globalStorageLock.unlock();
        }
    }

    private PartitionFile getPartitionFile(String topicName, int partitionIdx) {

        final String topicAndPartitionKey = "%s/%d".formatted(topicName, partitionIdx);

        if (topicAndPartitionToFile.containsKey(topicAndPartitionKey)) {
            System.out.printf("[%s]Getting PartitionFile from in-memory hash", config.brokerName());
            return topicAndPartitionToFile.get(topicAndPartitionKey);
        }
        /*
        <brokerDataFolder> = ./data/<broker id>
        <brokerDataFolder>/<topic-name>/partition-<partition idx>/0000000000.log <-- write-ahead log file
        <brokerDataFolder>/<topic-name>/partition-<partition idx>/0000000000.index <-- index file (store mapping from message offset to file offset)
        */
        final Path topicFolder = Path.of(brokerDataFolder.toString(), topicName);

        IOUtils.createFolderIfNotExist(config.brokerName(), topicFolder);

        final Path partitionFolder =
                Path.of(topicFolder.toString(), "partition-%d".formatted(partitionIdx));
        IOUtils.createFolderIfNotExist(config.brokerName(), partitionFolder);

        // todo: use normal message offset here
        long messageOffset = 0L;
        final Path logFilePath =
                Path.of(partitionFolder.toString(), "%010d.log".formatted(messageOffset));
        final Path indexFilePath =
                Path.of(partitionFolder.toString(), "%010d.index".formatted(messageOffset));

        IOUtils.createFileIfNotExist(config.brokerName(), logFilePath);
        IOUtils.createFileIfNotExist(config.brokerName(), indexFilePath);

        RandomWritableFile writableLogFile = new RandomWritableFile(logFilePath);
        RandomWritableFile writableIndexFile = new RandomWritableFile(indexFilePath);

        PartitionFile partitionFile = new PartitionFile(writableLogFile, writableIndexFile);

        System.out.printf("[%s]Saving PartitionFile in-memory", config.brokerName());
        topicAndPartitionToFile.put(topicAndPartitionKey, partitionFile);

        return partitionFile;
    }
}
