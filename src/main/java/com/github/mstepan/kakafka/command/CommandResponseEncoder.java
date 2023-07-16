package com.github.mstepan.kakafka.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.charset.StandardCharsets;

public final class CommandResponseEncoder extends MessageToByteEncoder<CommandResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, CommandResponse msg, ByteBuf out) {
        out.writeInt(msg.data().length());
        out.writeCharSequence(msg.data(), StandardCharsets.US_ASCII);
    }
}
