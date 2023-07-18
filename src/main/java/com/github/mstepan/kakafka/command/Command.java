package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public interface Command {

    CommandMarker marker();

    void encode(DataOut out);

    static Command decode(DataIn in, int typeMarker) {

        CommandMarker marker = CommandMarker.fromIntValue(typeMarker);

        return switch (marker) {
            case EXIT -> new ExitCommand();
            case GET_METADATA -> new GetMetadataCommand();
            case CREATE_TOPIC -> new CreateTopicCommand(in);
        };
    }
}
