package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.io.IOUtils;
import java.nio.file.Path;
import java.util.Objects;

/*
 * There should be ONE LogStorage associated with main broker process.
 * All public method from this class should be threads safe b/c will be executed by multiple threads.
 */
public final class LogStorage implements AutoCloseable {

    private final Object mutex = new Object();

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
    public void appendMessage(String topicName, int partitionIdx, StringTopicMessage msg) {

        PartitionFile partitionFile = registry.get(topicName, partitionIdx);

        // PartitionFile not found in registry so far so there are 2 possible scenarios:
        // 1. File doesn't exist at all.
        // 2. File wasn't added to cache before.
        if (partitionFile == null) {

            TopicPartitionPaths topicPartitionPaths =
                    constructPartitionPaths(topicName, partitionIdx);

            // Check if 'log' and 'index' files are exist and create if needed using critical
            // section to prevent concurrent creation of files.
            synchronized (mutex) {

                // check if another thread created PartitionFile before we entered critical section
                partitionFile = registry.get(topicName, partitionIdx);

                if (partitionFile == null) {
                    if (!topicPartitionPaths.isLogFileExist()) {
                        IOUtils.createFileIfNotExist(topicPartitionPaths.logFilePath());
                    }
                    if (!topicPartitionPaths.isIndexFileExist()) {
                        IOUtils.createFileIfNotExist(topicPartitionPaths.indexFilePath());
                    }

                    partitionFile = new PartitionFile(topicPartitionPaths);
                    registry.put(topicName, partitionIdx, partitionFile);
                }
            }
        }

        partitionFile.lock();

        try {
            partitionFile.appendMessage(msg);
        } finally {
            partitionFile.unlock();
        }
    }

    /*
     * Read message identified by triplet <topic name, partition index, message offset>.
     */
    public StringTopicMessage getMessage(String topicName, int partitionIdx, int msgIdx) {

        PartitionFile partitionFile = registry.get(topicName, partitionIdx);

        if (partitionFile == null) {

            TopicPartitionPaths topicPartitionPaths =
                    constructPartitionPaths(topicName, partitionIdx);

            if (topicPartitionPaths.isBothFilesExist()) {
                synchronized (mutex) {
                    // check if someone else already entered critical section and added
                    // PartitionFile
                    partitionFile = registry.get(topicName, partitionIdx);
                    if (partitionFile == null) {
                        partitionFile = new PartitionFile(topicPartitionPaths);
                        registry.put(topicName, partitionIdx, partitionFile);
                    }
                }
            } else {
                // log file or index file from PartitionFile doesn't exist, so can't read message
                return null;
            }
        }

        return partitionFile.readMessage(msgIdx);
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
