package com.github.mstepan.kakafka.command;

public interface CommandResponse {

    int GET_METADATA_MARKER = Command.Type.GET_METADATA.marker();
}
