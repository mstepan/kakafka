package com.github.mstepan.kakafka.broker.core.storage;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * We should limit the number of open files here, b/c there is UPPER limit for files descriptors per
 * process. For MacOS it's quite high `ulimit -n` = 65536
 *
 * <p>To check the number of open files per JVM process we can do the following:
 *
 * <p>1. `jps` find all java processes.
 *
 * <p>2. `lsof -p <PID> | wc -l` get number of open file descriptors.
 */
public class PartitionFileRegistry {

    private final ConcurrentMap<String, PartitionFile> cache = new ConcurrentHashMap<>();

    public PartitionFile get(String topic, int partitionIdx) {
        return cache.get(topicAndPartitionKey(topic, partitionIdx));
    }

    public void put(String topic, int partitionIdx, PartitionFile partitionFile) {
        cache.put(topicAndPartitionKey(topic, partitionIdx), partitionFile);
    }

    private static String topicAndPartitionKey(String topicName, int partitionIdx) {
        return "%s/%d".formatted(topicName, partitionIdx);
    }

    public Collection<PartitionFile> getAllPartitionFiles() {
        return cache.values();
    }
}
