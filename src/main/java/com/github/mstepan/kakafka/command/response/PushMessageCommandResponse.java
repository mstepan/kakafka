package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record PushMessageCommandResponse(int status) implements CommandResponse {

    @Override
    public void encode(DataOut out) {
        // | MARKER, int |
        out.writeInt(CommandMarker.PUSH_MESSAGE.value());

        // | status, int |
        out.writeInt(status);
    }

    public static PushMessageCommandResponse decode(DataIn in) {
        //  | MARKER, int |
        //  will be decoded inside CommandResponseDecoder.decoded

        // | status, int |
        int statusCode = in.readInt();

        return new PushMessageCommandResponse(statusCode);
    }
}
