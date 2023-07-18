package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record CreateTopicCommandResponse() implements CommandResponse {

    @Override
    public void encode(DataOut out) {
        // | MARKER, int |
        out.writeInt(CommandMarker.CREATE_TOPIC.value());
    }

    public static CreateTopicCommandResponse decode(DataIn in) {
        return new CreateTopicCommandResponse();
    }
}
