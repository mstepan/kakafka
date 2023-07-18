package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record ExitCommand() implements Command {

    @Override
    public void encode(DataOut out) {
        out.writeInt(CommandMarker.EXIT.value());
    }

    public static ExitCommand decode(DataIn in) {
        return new ExitCommand();
    }
}
