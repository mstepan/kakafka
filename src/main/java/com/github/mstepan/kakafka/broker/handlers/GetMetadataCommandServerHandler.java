package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.utils.BrokerMdcPropagator;
import com.github.mstepan.kakafka.broker.utils.Preconditions;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            try (BrokerMdcPropagator notUsed = new BrokerMdcPropagator(brokerName)) {
                LOG.info("'get_metadata' command received");
                MetadataState state = metadata.getMetadataState();
                ctx.writeAndFlush(new MetadataCommandResponse(state, 200));
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
        try (BrokerMdcPropagator notUsed = new BrokerMdcPropagator(brokerName)) {
            LOG.error("Exception during GetMetadataCommandServerHandler call", ex);
        } finally {
            ctx.close();
        }
    }
}
