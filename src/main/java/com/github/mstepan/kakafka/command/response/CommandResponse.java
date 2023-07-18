package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.io.DataOut;

public interface CommandResponse {

    void encode(DataOut out);
}
