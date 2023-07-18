package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;

public final class CommandResponseDecoder extends ReplayingDecoder<Void> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) {
        DataIn in = DataIn.fromNettyByteBuf(buf);
        out.add(decode(in));
    }

    public static CommandResponse decode(DataIn in) {
        // read marker 'int' value
        int marker = in.readInt();

        if (marker == CommandMarker.GET_METADATA.value()) {
            return MetadataCommandResponse.decode(in);
        } else {
            throw new IllegalStateException("Unknown marker type detected: " + marker);
        }
    }
}
