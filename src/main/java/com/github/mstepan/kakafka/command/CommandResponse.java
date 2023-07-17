package com.github.mstepan.kakafka.command;

public interface CommandResponse {

    int GET_METADATA_MARKER = KakafkaCommand.Type.GET_METADATA.marker();
}
