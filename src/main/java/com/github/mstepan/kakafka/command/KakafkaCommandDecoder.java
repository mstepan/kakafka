package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;

public final class KakafkaCommandDecoder extends ReplayingDecoder<Void> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        out.add(decode(DataIn.fromNettyByteBuf(in)));
    }

    public static KakafkaCommand decode(DataIn in) {
        int typeMarker = in.readInt();
        return new KakafkaCommand(KakafkaCommand.Type.fromMarker(typeMarker));
    }
}
