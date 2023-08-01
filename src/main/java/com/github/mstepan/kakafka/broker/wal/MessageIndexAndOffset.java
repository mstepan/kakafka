package com.github.mstepan.kakafka.broker.wal;

public record MessageIndexAndOffset(long msgIdx, long fileOffset) {}
