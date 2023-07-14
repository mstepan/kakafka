package com.github.mstepan.kakafka.broker.command;

import com.github.mstepan.kakafka.dto.KakafkaCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;

public class CommandServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            KakafkaCommand command = (KakafkaCommand) msg;

            if (command.type() == KakafkaCommand.Type.EXIT) {
                System.out.println("'exit' command received");
                ctx.close();
            } else if (command.type() == KakafkaCommand.Type.GET_METADATA) {
                System.out.println("'get_metadata' command received");

                ByteBuf buf = ctx.alloc().buffer(metadataMock().length());
                buf.writeBytes(metadataMock().getBytes(StandardCharsets.US_ASCII));
                ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private static String metadataMock() {
        return """
            {
                "brokers":
                    {
                        "id": "broker-0",
                        "host": "localhost:9091"
                    }
            }
            """;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) throws Exception {
        ex.printStackTrace();
        ctx.close();
    }
}
