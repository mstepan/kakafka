package com.github.mstepan.kakafka.client.time;

import com.github.mstepan.kakafka.dto.UnixTime;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class TimeClientHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            UnixTime timeUnix = (UnixTime) msg;
            System.out.printf("Time received: %s%n", timeUnix.toString());
            ctx.close();
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
