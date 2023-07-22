package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import java.util.Objects;

public record PushMessageCommand(StringTopicMessage msg) implements Command {

    public PushMessageCommand {
        Objects.requireNonNull(msg, "null 'msg' detected");
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
        out.writeString(getMsgKey());
        out.writeString(getMsgValue());
    }

    public static PushMessageCommand decode(DataIn in) {
        // read 'message.key' string
        String messageKey = in.readString();

        // read 'message.value' string
        String messageValue = in.readString();

        return new PushMessageCommand(new StringTopicMessage(messageKey, messageValue));
    }
}
