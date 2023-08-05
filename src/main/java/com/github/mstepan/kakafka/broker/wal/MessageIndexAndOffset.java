package com.github.mstepan.kakafka.broker.wal;

public record MessageIndexAndOffset(int msgIdx, long fileOffset) {}
