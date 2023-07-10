package com.github.mstepan.kakafka.broker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.nio.charset.StandardCharsets;

@ChannelHandler.Sharable
public class EchoServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf data = (ByteBuf) msg;

        String command = data.toString(StandardCharsets.UTF_8);

        if ("get_metadata".equals(command)) {
            System.out.println("Retrieving Metadata from Zookeeper");
            System.out.println("Sending metadata to client");

            ctx.writeAndFlush(Unpooled.copiedBuffer(
        """
        {
            "broker": [
                {
                    "id": 1,
                    "address": "localhost:9091"
                },
                {
                    "id": 2,
                    "address": "localhost:9092"
                },
                {
                    "id": 3,
                    "address": "localhost:9093"
                }
                ]
            "leaderId": "2"
        }
        """, StandardCharsets.UTF_8));
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) throws Exception {
        ex.printStackTrace();
        ctx.close();
    }
}
