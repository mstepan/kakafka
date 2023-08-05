package com.github.mstepan.kakafka.broker.core.storage;

public record MessageStreamStatus(int msgIdx, long fileOffset) {

    public static final MessageStreamStatus START = new MessageStreamStatus(0, 0);
}
