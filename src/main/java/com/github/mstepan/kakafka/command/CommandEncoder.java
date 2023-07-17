package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataOut;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class CommandEncoder extends MessageToByteEncoder<Command> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Command msg, ByteBuf buf) {
        encode(DataOut.fromNettyByteBuf(buf), msg);
    }

    public static void encode(DataOut out, Command msg) {
        out.writeInt(msg.marker().value());
        msg.encode(out);
    }
}
