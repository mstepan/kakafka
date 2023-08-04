package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.wal.LogStorage;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.ConsumeMessageCommand;
import com.github.mstepan.kakafka.command.response.ConsumeMessageCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public final class ConsumeMessageServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    private final LogStorage logStorage;

    public ConsumeMessageServerHandler(String brokerName, LogStorage logStorage) {
        this.brokerName = brokerName;
        this.logStorage = logStorage;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Command command = (Command) msg;

        if (command instanceof ConsumeMessageCommand consumeMsgCommand) {
            try {
                System.out.printf(
                        "[%s] 'consumeMessage' command, topic = '%s' partition idx = '%d', offset = '%d' %n",
                        brokerName,
                        consumeMsgCommand.topicName(),
                        consumeMsgCommand.partitionsIdx(),
                        consumeMsgCommand.offset());

                ctx.writeAndFlush(new ConsumeMessageCommandResponse("key-fake", "fake-value", 200));
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
