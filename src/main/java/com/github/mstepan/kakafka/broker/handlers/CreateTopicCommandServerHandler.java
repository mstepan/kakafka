package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CreateTopicCommand;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class CreateTopicCommandServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    public CreateTopicCommandServerHandler(String brokerName) {
        this.brokerName = brokerName;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        Command command = (Command) msg;

        if (command instanceof CreateTopicCommand createTopic) {
            try {
                System.out.printf(
                        "[%s] 'create_topic'command for topic '%s' with '%d' partitions %n",
                        brokerName, createTopic.topicName(), createTopic.partitionsCount());

                ctx.writeAndFlush(new CreateTopicCommandResponse());
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
