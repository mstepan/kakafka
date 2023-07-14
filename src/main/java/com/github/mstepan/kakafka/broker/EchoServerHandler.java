package com.github.mstepan.kakafka.broker;

import static io.netty.util.CharsetUtil.US_ASCII;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

@ChannelHandler.Sharable
public class EchoServerHandler extends ChannelInboundHandlerAdapter {

    private final String serverId;

    public EchoServerHandler(String serverId) {
        this.serverId = serverId;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            ByteBuf buf = (ByteBuf) msg;

            String command = buf.toString(US_ASCII).trim();

            System.out.printf("[%s] received: %s%n", serverId, command);

            if ("get_metadata".equals(command)) {
                System.out.println("Retrieving Metadata");
                System.out.println("Sending metadata to client");

                ctx.writeAndFlush(Unpooled.copiedBuffer(metadataMockedResponse(), US_ASCII));

            } else if ("exit".equals(command)) {
                System.out.println("Disconnecting client");
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            } else {
                System.err.printf("Unknown command '%s'%n", command);
                ctx.writeAndFlush(
                        Unpooled.copiedBuffer(
                                "Unknown command: '%s'%n".formatted(command), US_ASCII));
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private String metadataMockedResponse() {
        return """
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
            """;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
        ex.printStackTrace();
        ctx.close();
    }
}
