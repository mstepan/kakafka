package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.MetadataState;

public record MetadataCommandResponse(MetadataState state) implements CommandResponse {}
