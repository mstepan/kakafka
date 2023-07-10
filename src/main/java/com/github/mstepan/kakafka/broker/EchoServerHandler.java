package com.github.mstepan.kakafka.broker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@ChannelHandler.Sharable
public class EchoServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final UUID serverId;

    public EchoServerHandler(UUID serverId) {
        this.serverId = serverId;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf data) throws Exception {

        String command = data.toString(StandardCharsets.UTF_8).trim();

        System.out.printf("Server[%s] received: %s%n", serverId.toString(), command);

        if ("get_metadata".equals(command)) {
            System.out.println("Retrieving Metadata from Zookeeper");
            System.out.println("Sending metadata to client");

            ctx.writeAndFlush(
                    Unpooled.copiedBuffer(
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
        """,
                            StandardCharsets.UTF_8));
        } else if ("exit".equals(command)) {
            System.out.println("Disconnecting client");
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        } else {
            System.err.printf("Unknown command '%s'%n", command);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) throws Exception {
        ex.printStackTrace();
        ctx.close();
    }
}
