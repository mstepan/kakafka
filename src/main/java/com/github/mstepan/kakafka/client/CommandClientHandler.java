package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.command.GetMetadataResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class CommandClientHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof GetMetadataResponse metaResp) {
                System.out.printf("leader broker: %s%n", metaResp.state().leaderBrokerName());

                for (LiveBroker singleBroker : metaResp.state().brokers()) {
                    System.out.printf(
                            "broker-id: %s, url: %s %n", singleBroker.id(), singleBroker.url());
                }

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
