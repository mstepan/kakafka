package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public final class CommandClientHandler extends ChannelInboundHandlerAdapter {

    private final SynchChannel<MetadataState> metaChannel;

    public CommandClientHandler(SynchChannel<MetadataState> metaChannel) {
        this.metaChannel = metaChannel;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof MetadataCommandResponse metaResp) {
                metaChannel.set(metaResp.state());
            } else {
                System.err.println("Unknown response detected");
            }
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
