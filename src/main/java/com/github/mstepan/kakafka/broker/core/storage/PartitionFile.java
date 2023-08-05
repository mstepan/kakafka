package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.wal.MessageStreamStatus;
import com.github.mstepan.kakafka.io.RandomWritableFile;

public final class PartitionFile {

    private final RandomWritableFile log;
    private final RandomWritableFile index;

    private volatile MessageStreamStatus lastMsgIdx;

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

    public MessageStreamStatus streamStatus() {
        if (lastMsgIdx == null) {
            lastMsgIdx = index.readLastMessageIndexAndOffset();

            if (lastMsgIdx == null) {
                lastMsgIdx = MessageStreamStatus.START;
            }
        }

        return lastMsgIdx;
    }

    public void updateStreamStatus(int msgIdx, long fileOffset) {
        lastMsgIdx = new MessageStreamStatus(msgIdx, fileOffset);
    }

    /** Read index file to find the mapping between 'message idx' => 'log file offset' */
    public StringTopicMessage readMessage(int msgIdx) {
        try {
            index.moveToStart();
            MessageStreamStatus idxAndOffset = index.findMessageOffset(msgIdx);

            if (idxAndOffset != null) {
                try {
                    // read message from 'log' file according to found 'offset'
                    return log.readMessageByOffset(idxAndOffset.fileOffset());
                } finally {
                    log.moveToEnd();
                }
            }
        } finally {
            index.moveToEnd();
        }

        return null;
    }
}
