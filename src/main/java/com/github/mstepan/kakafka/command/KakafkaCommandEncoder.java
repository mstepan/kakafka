package com.github.mstepan.kakafka.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class KakafkaCommandEncoder extends MessageToByteEncoder<KakafkaCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, KakafkaCommand msg, ByteBuf out) {
        out.writeInt(msg.type().marker());
    }
}
