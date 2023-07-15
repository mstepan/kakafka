package com.github.mstepan.kakafka.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CommandResponseDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

        if (in.readableBytes() < Integer.BYTES) {
            return;
        }

        int responseLength = in.readInt();

        if (in.readableBytes() < responseLength) {
            return;
        }

        byte[] respData = new byte[responseLength];
        in.readBytes(respData);

        out.add(new CommandResponse(new String(respData, StandardCharsets.US_ASCII)));
    }
}
