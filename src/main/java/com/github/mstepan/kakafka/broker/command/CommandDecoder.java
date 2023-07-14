package com.github.mstepan.kakafka.broker.command;

import com.github.mstepan.kakafka.dto.KakafkaCommand;
import com.github.mstepan.kakafka.dto.UnixTime;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;


public class CommandDecoder extends ReplayingDecoder<Void> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int typeMarker = in.readInt();
        out.add(new KakafkaCommand(KakafkaCommand.Type.fromMarker(typeMarker)));
    }
}
