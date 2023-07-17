package com.github.mstepan.kakafka.command;

public record ExitCommand() implements Command {

    @Override
    public CommandMarker marker() {
        return CommandMarker.EXIT;
    }
}
