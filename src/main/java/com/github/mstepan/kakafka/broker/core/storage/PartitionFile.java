package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.broker.wal.MessageIndexAndOffset;
import com.github.mstepan.kakafka.io.RandomWritableFile;

public final class PartitionFile {

    private final RandomWritableFile log;
    private final RandomWritableFile index;

    private volatile MessageIndexAndOffset lastMsgIdx;

    public PartitionFile(RandomWritableFile log, RandomWritableFile index) {
        this.log = log;
        this.index = index;
    }

    public RandomWritableFile index() {
        return index;
    }

    public RandomWritableFile log() {
        return log;
    }

    public MessageIndexAndOffset lastMessageIdxAndOffset() {
        if (lastMsgIdx == null) {
            lastMsgIdx = index.readLastMessageIndexAndOffset();

            if (lastMsgIdx == null) {
                lastMsgIdx = new MessageIndexAndOffset(0, 0);
            }
        }

        return lastMsgIdx;
    }

    public void updateLastMessageIdxAndOffset(long msgIdx, long fileOffset) {
        lastMsgIdx = new MessageIndexAndOffset(msgIdx, fileOffset);
    }
}
