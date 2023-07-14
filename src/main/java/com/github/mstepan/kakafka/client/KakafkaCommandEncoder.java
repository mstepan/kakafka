package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.dto.KakafkaCommand;
import com.github.mstepan.kakafka.dto.UnixTime;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class KakafkaCommandEncoder extends MessageToByteEncoder<KakafkaCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, KakafkaCommand msg, ByteBuf out) {
        out.writeInt(msg.type().marker());
    }
}
