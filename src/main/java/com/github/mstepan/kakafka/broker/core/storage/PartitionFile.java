package com.github.mstepan.kakafka.broker.core.storage;

import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.io.Preconditions;
import com.github.mstepan.kakafka.io.RandomWritableFile;


public final class PartitionFile implements AutoCloseable {

    private final RandomWritableFile log;
    private final RandomWritableFile index;

    private volatile MessageStreamStatus streamStatus;

    public PartitionFile(RandomWritableFile log, RandomWritableFile index) {
        this.log = log;
        this.index = index;
    }

    public PartitionFile(TopicPartitionPaths paths) {
        Preconditions.checkArgument(paths != null, "null 'paths' detected");
        this.log = new RandomWritableFile(paths.logFilePath());
        this.index = new RandomWritableFile(paths.indexFilePath());
    }

    public void appendMessage(StringTopicMessage msg) {

        MessageStreamStatus fileStreamStatus = getStreamStatus();

        long newOffset = log.appendKeyAndValue(msg.key(), msg.value());

        index.appendMessageOffset(fileStreamStatus.msgIdx(), fileStreamStatus.fileOffset());

        streamStatus = new MessageStreamStatus(fileStreamStatus.msgIdx() + 1, newOffset);
    }

    public MessageStreamStatus getStreamStatus() {
        if (streamStatus == null) {
            streamStatus = index.readLastMessageIndexAndOffset();

            if (streamStatus == null) {
                streamStatus = MessageStreamStatus.START;
            }
        }

        return streamStatus;
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

    @Override
    public void close() throws Exception {
        log.close();
        index.close();
    }
}
