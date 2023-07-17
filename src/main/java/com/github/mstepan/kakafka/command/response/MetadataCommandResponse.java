package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.broker.core.MetadataState;

public record MetadataCommandResponse(MetadataState state) implements CommandResponse {}
