package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record GetMetadataCommand() implements Command {

    @Override
    public void encode(DataOut out) {
        out.writeInt(CommandMarker.GET_METADATA.value());
    }

    public static GetMetadataCommand decode(DataIn in) {
        return new GetMetadataCommand();
    }
}
