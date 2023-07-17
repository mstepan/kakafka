package com.github.mstepan.kakafka.command;

public interface Command {

    default Command metadataCommand() {
        return new GetMetadataCommand();
    }

    default Command exitCommand() {
        return new ExitCommand();
    }

    CommandMarker marker();
}
