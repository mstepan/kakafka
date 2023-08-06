package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.utils.BrokerMdcPropagator;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.ExitCommand;
import com.github.mstepan.kakafka.command.response.ExitCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ExitCommandServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String brokerName;

    public ExitCommandServerHandler(BrokerContext brokerCtx) {
        this.brokerName = brokerCtx.config().brokerName();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Command command = (Command) msg;

        if (command instanceof ExitCommand) {
            try {
                LOG.info("'exit' command received");
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
        try (BrokerMdcPropagator notUsed = new BrokerMdcPropagator(brokerName)) {
            LOG.error("Exit command handler failed", ex);
        }
        finally {
            ctx.close();
        }
    }
}
