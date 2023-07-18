package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataOut;

public interface Command {

    CommandMarker marker();

    void encode(DataOut out);
}
