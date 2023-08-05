package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.io.IOUtils;
import com.github.mstepan.kakafka.io.RandomWritableFile;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/*
* There should be ONE LogStorage associated with main broker process.
* All public method from this class should be threads safe b/c will be executed by multiple threads.
* Rigth now we will just use 'synchronized' keyword for simplicity.
*/
public final class LogStorage {

    private final BrokerConfig config;

    private final Path brokerDataFolder;

    private final Map<String, PartitionFile> topicPartitionFileCache = new HashMap<>();

    public LogStorage(BrokerConfig config) {
        this.config = Objects.requireNonNull(config, "null 'config' detected");
        this.brokerDataFolder = Path.of(config.dataFolder(), config.brokerName());
    }

    /*
     * Write to local broker FS. Append message to end of write-ahead log (WAL) and update index file properly.
     */
    public synchronized void appendMessage(
            String topicName, int partitionIdx, StringTopicMessage msg) {

        // Create broker data folder lazily during first append operation
        IOUtils.createFolderIfNotExist(config.brokerName(), brokerDataFolder);

        PartitionFile partitionFile = getPartitionFile(topicName, partitionIdx, true);
        partitionFile.appendMessage(msg);
    }

    /*
     * Read message identified by triplet <topic name, partition index, message offset>.
     */
    public synchronized StringTopicMessage getMessage(
            String topicName, int partitionIdx, int msgIdx) {
        PartitionFile partitionFile = getPartitionFile(topicName, partitionIdx, false);

        if (partitionFile == null) {
            // partition file not found, so can't read message
            return null;
        }

        return partitionFile.readMessage(msgIdx);
    }

    private PartitionFile getPartitionFile(
            String topicName, int partitionIdx, boolean createIfNotExist) {

        final String topicAndPartitionKey = topicAndPartitionKey(topicName, partitionIdx);

        PartitionFile partitionFileFromCache = topicPartitionFileCache.get(topicAndPartitionKey);

        if (partitionFileFromCache != null) {
            System.out.printf(
                    "[%s]Getting PartitionFile from in-memory hash%n", config.brokerName());
            return partitionFileFromCache;
        }

        TopicPartitionPaths topicPartitionPaths = constructPartitionPaths(topicName, partitionIdx);

        if (createIfNotExist) {
            if (!IOUtils.exist(topicPartitionPaths.logFilePath())) {
                IOUtils.createFileIfNotExist(topicPartitionPaths.logFilePath());
            }

            if (!IOUtils.exist(topicPartitionPaths.indexFilePath())) {
                IOUtils.createFileIfNotExist(topicPartitionPaths.indexFilePath());
            }
        }

        if (!IOUtils.exist(topicPartitionPaths.logFilePath())
                || !IOUtils.exist(topicPartitionPaths.indexFilePath())) {
            return null;
        }

        RandomWritableFile writableLogFile =
                new RandomWritableFile(topicPartitionPaths.logFilePath());
        RandomWritableFile writableIndexFile =
                new RandomWritableFile(topicPartitionPaths.indexFilePath());

        PartitionFile partitionFile = new PartitionFile(writableLogFile, writableIndexFile);

        System.out.printf("[%s]Saving PartitionFile in-memory%n", config.brokerName());
        topicPartitionFileCache.put(topicAndPartitionKey, partitionFile);

        return partitionFile;
    }

    private static String topicAndPartitionKey(String topicName, int partitionIdx) {
        return "%s/%d".formatted(topicName, partitionIdx);
    }

    /**
     * Construct full path to log file and index file using provided 'topicName' and 'partitionIdx'.
     */
    private TopicPartitionPaths constructPartitionPaths(String topicName, int partitionIdx) {
        final Path topicFolder = Path.of(brokerDataFolder.toString(), topicName);

        final Path partitionFolder =
                Path.of(topicFolder.toString(), "partition-%d".formatted(partitionIdx));

        // todo: use normal message offset here
        long lastMessageIdx = 0L;
        final Path logFilePath =
                Path.of(partitionFolder.toString(), "%010d.log".formatted(lastMessageIdx));
        final Path indexFilePath =
                Path.of(partitionFolder.toString(), "%010d.index".formatted(lastMessageIdx));

        return new TopicPartitionPaths(logFilePath, indexFilePath);
    }
}
