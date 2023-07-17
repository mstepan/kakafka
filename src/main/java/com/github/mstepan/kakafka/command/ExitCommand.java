package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;

public record ExitCommand() implements Command {

    public ExitCommand() {}

    public ExitCommand(DataIn in) {
        this();
    }

    @Override
    public CommandMarker marker() {
        return CommandMarker.EXIT;
    }
}
