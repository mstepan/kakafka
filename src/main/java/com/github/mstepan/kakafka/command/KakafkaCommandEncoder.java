package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataOut;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class KakafkaCommandEncoder extends MessageToByteEncoder<KakafkaCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, KakafkaCommand msg, ByteBuf buf) {
        buf.writeInt(msg.type().marker());
        encode(DataOut.fromNettyByteBuf(buf), msg);
    }

    public static void encode(DataOut out, KakafkaCommand msg) {
        out.writeInt(msg.type().marker());
    }
}
