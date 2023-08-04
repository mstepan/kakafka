package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record ConsumeMessageCommandResponse(String key, String value, int status)
        implements CommandResponse {

    @Override
    public void encode(DataOut out) {
        // | MARKER, int |
        out.writeInt(CommandMarker.CONSUME_MESSAGE.value());

        // | status, int |
        out.writeInt(status);

        if (status != 200) {
            return;
        }

        // | key as string |
        out.writeString(key);

        // | value as string |
        out.writeString(value);
    }

    public static ConsumeMessageCommandResponse decode(DataIn in) {
        //  | MARKER, int |
        //  will be decoded inside CommandResponseDecoder.decoded

        // | status, int |
        int status = in.readInt();

        if (status != 200) {
            return new ConsumeMessageCommandResponse(null, null, status);
        }

        String key = in.readString();
        String value = in.readString();

        return new ConsumeMessageCommandResponse(key, value, status);
    }
}
