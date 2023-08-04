package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record ConsumeMessageCommand(String topicName, int partitionsIdx, long offset)
        implements Command {

    @Override
    public void encode(DataOut out) {
        out.writeInt(CommandMarker.CONSUME_MESSAGE.value());
        out.writeString(topicName);
        out.writeInt(partitionsIdx);
        out.writeLong(offset);
    }

    public static ConsumeMessageCommand decode(DataIn in) {

        // read 'topicName' as string
        String topicName = in.readString();

        // read 'partitionIdx' as int
        int partitionIdx = in.readInt();

        // read 'msg offset' as long
        long msgOffset = in.readLong();

        return new ConsumeMessageCommand(topicName, partitionIdx, msgOffset);
    }
}
