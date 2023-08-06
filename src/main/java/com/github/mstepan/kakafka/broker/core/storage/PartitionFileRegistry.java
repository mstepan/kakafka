package com.github.mstepan.kakafka.broker.core.storage;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
