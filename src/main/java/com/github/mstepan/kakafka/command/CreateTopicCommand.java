package com.github.mstepan.kakafka.command;

public record CreateTopicCommand() implements Command {

    @Override
    public CommandMarker marker() {
        return CommandMarker.CREATE_TOPIC;
    }
}
