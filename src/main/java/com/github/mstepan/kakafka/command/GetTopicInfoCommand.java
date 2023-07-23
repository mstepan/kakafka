package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record GetTopicInfoCommand(String topicName) implements Command {

    @Override
    public void encode(DataOut out) {
        out.writeInt(CommandMarker.GET_TOPIC_INFO.value());
        out.writeString(topicName);
    }

    public static GetTopicInfoCommand decode(DataIn in) {
        return new GetTopicInfoCommand(in.readString());
    }
}
