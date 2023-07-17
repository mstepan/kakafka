package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;

public record GetMetadataCommand() implements Command {

    public GetMetadataCommand() {}

    public GetMetadataCommand(DataIn in) {
        this();
    }

    @Override
    public CommandMarker marker() {
        return CommandMarker.GET_METADATA;
    }
}
