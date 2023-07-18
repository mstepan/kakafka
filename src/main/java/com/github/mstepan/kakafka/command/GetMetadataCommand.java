package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataOut;

public record GetMetadataCommand() implements Command {

    @Override
    public CommandMarker marker() {
        return CommandMarker.GET_METADATA;
    }

    @Override
    public void encode(DataOut out) {
        out.writeInt(marker().value());
    }
}
