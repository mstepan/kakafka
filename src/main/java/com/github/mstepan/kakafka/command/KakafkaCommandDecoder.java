package com.github.mstepan.kakafka.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;

public final class KakafkaCommandDecoder extends ReplayingDecoder<Void> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int typeMarker = in.readInt();
        out.add(new KakafkaCommand(KakafkaCommand.Type.fromMarker(typeMarker)));
    }
}
