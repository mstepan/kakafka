package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.MetadataState;

public record GetMetadataResponse(MetadataState state) implements CommandResponse {}
