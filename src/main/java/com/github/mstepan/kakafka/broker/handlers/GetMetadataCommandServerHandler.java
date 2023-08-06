package com.github.mstepan.kakafka.broker.handlers;

import static com.github.mstepan.kakafka.broker.BrokerMain.BROKER_NAME_MDC_KEY;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.utils.Preconditions;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public final class GetMetadataCommandServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String brokerName;
    private final MetadataStorage metadata;

    public GetMetadataCommandServerHandler(BrokerContext brokerContext) {
        Preconditions.checkNotNull(brokerContext, "null 'brokerContext' detected");
        this.brokerName =
                Preconditions.checkNotNull(brokerContext.config(), "null 'broker config' detected")
                        .brokerName();
        this.metadata =
                Preconditions.checkNotNull(
                        brokerContext.metadata(), "null 'broker metadata' detected");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        Command command = (Command) msg;

        if (command instanceof GetMetadataCommand) {

            MDC.put(BROKER_NAME_MDC_KEY, brokerName);
            try {
                LOG.info("'get_metadata' command received");
                MetadataState state = metadata.getMetadataState();
                ctx.writeAndFlush(new MetadataCommandResponse(state, 200));
            } finally {
                MDC.clear();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
        LOG.error("Exception during GetMetadataCommandServerHandler call", ex);
        ctx.close();
    }
}
