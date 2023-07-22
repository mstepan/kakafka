package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.wal.LogStorage;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.PushMessageCommand;
import com.github.mstepan.kakafka.command.response.PushMessageCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public final class PushMessageServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    private final LogStorage logStorage;

    public PushMessageServerHandler(String brokerName, LogStorage logStorage) {
        this.brokerName = brokerName;
        this.logStorage = logStorage;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Command command = (Command) msg;

        if (command instanceof PushMessageCommand pushCommand) {
            try {
                System.out.printf(
                        "[%s] 'pushMessage' topic '%s', partition idx '%d', (key = '%s', value = '%s')%n",
                        brokerName,
                        pushCommand.topicName(),
                        pushCommand.partitionsIdx(),
                        pushCommand.getMsgKey(),
                        pushCommand.getMsgValue());

                // TODO: write to local broker FS
                // TODO: append message to end of write-ahead log (WAL)

                logStorage.appendMessage(
                        pushCommand.topicName(), pushCommand.partitionsIdx(), pushCommand.msg());

                ctx.writeAndFlush(new PushMessageCommandResponse(200));
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
