package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.io.DataOut;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class CommandResponseEncoder extends MessageToByteEncoder<CommandResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, CommandResponse msg, ByteBuf buf) {
        try {
            DataOut out = DataOut.fromNettyByteBuf(buf);
            msg.encode(out);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
