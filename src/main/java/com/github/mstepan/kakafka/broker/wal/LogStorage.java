package com.github.mstepan.kakafka.broker.wal;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.storage.PartitionFile;
import com.github.mstepan.kakafka.broker.core.storage.TopicPartitionFS;
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

    /*
     * Write to local broker FS. Append message to end of write-ahead log (WAL).
     */
    public void appendMessage(String topicName, int partitionIdx, StringTopicMessage msg) {
        globalStorageLock.lock();
        try {
            PartitionFile partitionFile = getPartitionFile(topicName, partitionIdx);

            MessageIndexAndOffset lastMsgIdx = partitionFile.lastMessageIdxAndOffset();

            RandomWritableFile writableLogFile = partitionFile.log();
            long newOffset = writableLogFile.appendKeyValue(msg.key(), msg.value());

            RandomWritableFile writableIndexFile = partitionFile.index();
            writableIndexFile.appendMessageOffset(lastMsgIdx.msgIdx() + 1L, newOffset);

            partitionFile.updateLastMessageIdxAndOffset(lastMsgIdx.msgIdx() + 1L, newOffset);
        } finally {
            globalStorageLock.unlock();
        }
    }

    private PartitionFile getPartitionFile(String topicName, int partitionIdx) {

        final String topicAndPartitionKey = "%s/%d".formatted(topicName, partitionIdx);

        if (topicAndPartitionToFile.containsKey(topicAndPartitionKey)) {
            System.out.printf(
                    "[%s]Getting PartitionFile from in-memory hash%n", config.brokerName());
            return topicAndPartitionToFile.get(topicAndPartitionKey);
        }

        TopicPartitionFS topicPartitionFS = getTopicAndPartitionFileSystem(topicName, partitionIdx);

        if (!IOUtils.exist(topicPartitionFS.logFilePath())) {
            IOUtils.createFileIfNotExist(topicPartitionFS.logFilePath());
        }

        if (!IOUtils.exist(topicPartitionFS.indexFilePath())) {
            IOUtils.createFileIfNotExist(topicPartitionFS.indexFilePath());
        }

        RandomWritableFile writableLogFile = new RandomWritableFile(topicPartitionFS.logFilePath());
        RandomWritableFile writableIndexFile =
                new RandomWritableFile(topicPartitionFS.indexFilePath());

        PartitionFile partitionFile = new PartitionFile(writableLogFile, writableIndexFile);

        System.out.printf("[%s]Saving PartitionFile in-memory%n", config.brokerName());
        topicAndPartitionToFile.put(topicAndPartitionKey, partitionFile);

        return partitionFile;
    }

    /**
     * Construct full path to log file and index file using provided 'topicName' and 'partitionIdx'.
     */
    private TopicPartitionFS getTopicAndPartitionFileSystem(String topicName, int partitionIdx) {
        final Path topicFolder = Path.of(brokerDataFolder.toString(), topicName);

        final Path partitionFolder =
                Path.of(topicFolder.toString(), "partition-%d".formatted(partitionIdx));

        // todo: use normal message offset here
        long lastMessageIdx = 0L;
        final Path logFilePath =
                Path.of(partitionFolder.toString(), "%010d.log".formatted(lastMessageIdx));
        final Path indexFilePath =
                Path.of(partitionFolder.toString(), "%010d.index".formatted(lastMessageIdx));

        return new TopicPartitionFS(logFilePath, indexFilePath);
    }

    public StringTopicMessage getMessage(String topicName, int partitionIdx, long offset) {
        return null;
    }
}
