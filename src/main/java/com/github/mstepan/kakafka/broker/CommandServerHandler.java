package com.github.mstepan.kakafka.broker;

import com.github.mstepan.kakafka.command.CommandResponse;
import com.github.mstepan.kakafka.command.KakafkaCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class CommandServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    public CommandServerHandler(String brokerName) {
        this.brokerName = brokerName;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            KakafkaCommand command = (KakafkaCommand) msg;

            if (command.type() == KakafkaCommand.Type.EXIT) {
                System.out.printf("[%s] 'exit' command received %n", brokerName);
                ctx.close();
            } else if (command.type() == KakafkaCommand.Type.GET_METADATA) {
                System.out.printf("[%s] 'get_metadata' command received %n", brokerName);
                ctx.writeAndFlush(new CommandResponse(metadataMock()));
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