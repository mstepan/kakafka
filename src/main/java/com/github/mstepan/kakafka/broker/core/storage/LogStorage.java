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
public final class LogStorage implements AutoCloseable {

    private final BrokerConfig config;

    private final Path brokerDataFolder;

    private final PartitionFileRegistry registry;

    public LogStorage(BrokerConfig config) {
        this.config = Objects.requireNonNull(config, "null 'config' detected");
        this.brokerDataFolder = Path.of(config.dataFolder(), config.brokerName());
        this.registry = new PartitionFileRegistry();
    }

    /**
     * Create broker data folder if not exists. Init method doesn't need to be synchronized b/c it
     * will be called from main thread before broker event loop is actually started.
     */
    public void init() {
        IOUtils.createFolderIfNotExist(config.brokerName(), brokerDataFolder);
    }

    /*
     * Write to local broker FS. Append message to end of write-ahead log (WAL) and update index file properly.
     */
    public synchronized void appendMessage(
            String topicName, int partitionIdx, StringTopicMessage msg) {
        PartitionFile partitionFile = getPartitionFileCreateIfNotExists(topicName, partitionIdx);
        partitionFile.appendMessage(msg);
    }

    /*
     * Read message identified by triplet <topic name, partition index, message offset>.
     */
    public synchronized StringTopicMessage getMessage(
            String topicName, int partitionIdx, int msgIdx) {
        PartitionFile partitionFile = getPartitionFile(topicName, partitionIdx);

        if (partitionFile == null) {
            // partition file not found, so can't read message
            return null;
        }

        return partitionFile.readMessage(msgIdx);
    }

    private PartitionFile getPartitionFileCreateIfNotExists(String topicName, int partitionIdx) {

        PartitionFile partitionFileFromCache = registry.get(topicName, partitionIdx);

        if (partitionFileFromCache != null) {
            System.out.printf(
                    "[%s]Getting PartitionFile from in-memory hash%n", config.brokerName());
            return partitionFileFromCache;
        }

        TopicPartitionPaths topicPartitionPaths = constructPartitionPaths(topicName, partitionIdx);

        if (!IOUtils.exist(topicPartitionPaths.logFilePath())) {
            IOUtils.createFileIfNotExist(topicPartitionPaths.logFilePath());
        }

        if (!IOUtils.exist(topicPartitionPaths.indexFilePath())) {
            IOUtils.createFileIfNotExist(topicPartitionPaths.indexFilePath());
        }

        if (!IOUtils.exist(topicPartitionPaths.logFilePath())
                || !IOUtils.exist(topicPartitionPaths.indexFilePath())) {
            return null;
        }

        PartitionFile partitionFile = new PartitionFile(topicPartitionPaths);

        System.out.printf("[%s]Saving PartitionFile in-memory%n", config.brokerName());
        registry.put(topicName, partitionIdx, partitionFile);

        return partitionFile;
    }

    private PartitionFile getPartitionFile(String topicName, int partitionIdx) {

        PartitionFile partitionFileFromCache = registry.get(topicName, partitionIdx);

        if (partitionFileFromCache != null) {
            System.out.printf(
                    "[%s]Getting PartitionFile from in-memory hash%n", config.brokerName());
            return partitionFileFromCache;
        }

        TopicPartitionPaths topicPartitionPaths = constructPartitionPaths(topicName, partitionIdx);

        if (!IOUtils.exist(topicPartitionPaths.logFilePath())
                || !IOUtils.exist(topicPartitionPaths.indexFilePath())) {
            return null;
        }

        PartitionFile partitionFile = new PartitionFile(topicPartitionPaths);

        System.out.printf("[%s]Saving PartitionFile in-memory%n", config.brokerName());
        registry.put(topicName, partitionIdx, partitionFile);

        return partitionFile;
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

    @Override
    public synchronized void close() {
        for (PartitionFile singleParFile : registry.getAllPartitionFiles()) {
            try {
                singleParFile.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
