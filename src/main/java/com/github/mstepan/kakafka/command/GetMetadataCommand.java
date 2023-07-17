package com.github.mstepan.kakafka.command;

public record GetMetadataCommand() implements Command {

    @Override
    public CommandMarker marker() {
        return CommandMarker.GET_METADATA;
    }
}
