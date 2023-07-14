package com.github.mstepan.kakafka.client.command;

import com.github.mstepan.kakafka.dto.CommandResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CommandResponseDecoder extends ReplayingDecoder<Void> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

        int responseLength = in.readInt();

        byte[] respData = new byte[responseLength];
        in.readBytes(respData);

        out.add(new CommandResponse(new String(respData, StandardCharsets.US_ASCII)));
    }
}
