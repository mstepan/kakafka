package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record CreateTopicCommand() implements Command {

    public CreateTopicCommand() {}

    public CreateTopicCommand(DataIn in) {
        this();
    }

    @Override
    public CommandMarker marker() {
        return CommandMarker.CREATE_TOPIC;
    }

    @Override
    public void encode(DataOut out) {
        out.writeInt(marker().value());
    }
}
