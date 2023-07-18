package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataOut;

public interface Command {

    void encode(DataOut out);
}
