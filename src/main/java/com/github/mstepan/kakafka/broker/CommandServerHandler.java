package com.github.mstepan.kakafka.broker;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.MetadataCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class CommandServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    private final MetadataStorage metadata;

    public CommandServerHandler(String brokerName, MetadataStorage metadata) {
        this.brokerName = brokerName;
        this.metadata = metadata;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            Command command = (Command) msg;

            if (command.type() == Command.Type.EXIT) {
                System.out.printf("[%s] 'exit' command received %n", brokerName);
                ctx.close();
            } else if (command.type() == Command.Type.GET_METADATA) {
                System.out.printf("[%s] 'get_metadata' command received %n", brokerName);

                MetadataState state = metadata.getMetadataState();

                System.out.printf("[%s] metadata state obtained from 'etcd' %n", brokerName);

                ctx.writeAndFlush(new MetadataCommandResponse(state));
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) throws Exception {
        ex.printStackTrace();
        ctx.close();
    }
}
