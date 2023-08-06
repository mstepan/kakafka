package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.utils.Preconditions;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record PushMessageCommand(String topicName, int partitionsIdx, StringTopicMessage msg)
        implements Command {

    public PushMessageCommand {
        Preconditions.checkNotNull(msg, "null 'msg' detected");
    }

    public String getMsgKey() {
        return msg.key();
    }

    public String getMsgValue() {
        return msg.value();
    }

    @Override
    public void encode(DataOut out) {
        out.writeInt(CommandMarker.PUSH_MESSAGE.value());
        out.writeString(topicName);
        out.writeInt(partitionsIdx);
        out.writeString(getMsgKey());
        out.writeString(getMsgValue());
    }

    public static PushMessageCommand decode(DataIn in) {

        // read 'topicName' as string
        String topicName = in.readString();

        // read 'partitionIdx' as int
        int partitionIdx = in.readInt();

        // read 'message.key' string
        String messageKey = in.readString();

        // read 'message.value' string
        String messageValue = in.readString();

        return new PushMessageCommand(
                topicName, partitionIdx, new StringTopicMessage(messageKey, messageValue));
    }
}
