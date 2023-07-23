package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.io.RandomWritableFile;

public record PartitionFile(RandomWritableFile log, RandomWritableFile index) {}
