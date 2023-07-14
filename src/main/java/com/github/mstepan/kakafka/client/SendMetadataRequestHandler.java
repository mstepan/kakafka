package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.dto.KakafkaCommand;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;

public class SendMetadataRequestHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("Sending GET_METADATA command");

        ctx.writeAndFlush(new KakafkaCommand(KakafkaCommand.Type.GET_METADATA));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            ByteBuf buf = (ByteBuf)msg;
            System.out.println(buf.toString(StandardCharsets.US_ASCII));
        }
        finally {
            ReferenceCountUtil.release(msg);
        }
    }
}
