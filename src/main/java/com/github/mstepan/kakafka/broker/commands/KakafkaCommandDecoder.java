package com.github.mstepan.kakafka.broker.commands;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;

public class KakafkaCommandDecoder extends ReplayingDecoder<Void> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

        int commandMarker = in.readByte();

        KakafkaCommand.Type type = KakafkaCommand.Type.fromMarker(commandMarker);

        out.add(new KakafkaCommand(type));
    }
}
