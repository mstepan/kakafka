package com.github.mstepan.kakafka.client.command;

import com.github.mstepan.kakafka.dto.KakafkaCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class CommandEncoder extends MessageToByteEncoder<KakafkaCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, KakafkaCommand msg, ByteBuf out) {
        out.writeInt(msg.type().marker());
    }
}
