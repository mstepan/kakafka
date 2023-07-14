package com.github.mstepan.kakafka.client.command;

import com.github.mstepan.kakafka.dto.CommandResponse;
import com.github.mstepan.kakafka.dto.KakafkaCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class SendCommandRequestHandler extends ChannelInboundHandlerAdapter {

    private final KakafkaCommand command;

    public SendCommandRequestHandler(KakafkaCommand command) {
        this.command = command;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.printf("Sending %s command", command.type());
        ctx.writeAndFlush(command);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            CommandResponse response = (CommandResponse) msg;
            System.out.printf("response: %s%n", response);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
        ex.printStackTrace();
        ctx.close();
    }
}
