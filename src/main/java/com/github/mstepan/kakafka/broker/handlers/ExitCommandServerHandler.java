package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.ExitCommand;
import com.github.mstepan.kakafka.command.response.ExitCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public final class ExitCommandServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    public ExitCommandServerHandler(String brokerName) {
        this.brokerName = brokerName;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Command command = (Command) msg;

        if (command instanceof ExitCommand) {
            try {
                System.out.printf("[%s] 'exit' command received %n", brokerName);
                ctx.writeAndFlush(new ExitCommandResponse());
                ctx.close();
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) throws Exception {
        ex.printStackTrace();
        ctx.close();
    }
}
