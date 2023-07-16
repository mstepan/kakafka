package com.github.mstepan.kakafka.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public final class KakafkaCommandDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < Integer.BYTES) {
            return;
        }

        int typeMarker = in.readInt();
        out.add(new KakafkaCommand(KakafkaCommand.Type.fromMarker(typeMarker)));
    }
}
