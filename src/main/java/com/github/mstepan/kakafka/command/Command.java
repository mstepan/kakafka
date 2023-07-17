package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public interface Command {

    CommandMarker marker();

    void encode(DataOut out);

    static Command decode(DataIn in) {
        int typeMarker = in.readInt();

        CommandMarker marker = CommandMarker.fromIntValue(typeMarker);

        return switch (marker) {
            case EXIT -> new ExitCommand(in);
            case GET_METADATA -> new GetMetadataCommand(in);
            case CREATE_TOPIC -> new CreateTopicCommand(in);
        };
    }
}
