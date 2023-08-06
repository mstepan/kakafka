package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.storage.LogStorage;
import com.github.mstepan.kakafka.broker.utils.BrokerMdcPropagator;
import com.github.mstepan.kakafka.broker.utils.Preconditions;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.PushMessageCommand;
import com.github.mstepan.kakafka.command.response.PushMessageCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PushMessageServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String brokerName;
    private final LogStorage logStorage;

    public PushMessageServerHandler(BrokerContext brokerCtx) {
        Preconditions.checkNotNull(brokerCtx, "null 'brokerCtx' detected");
        this.brokerName =
                Preconditions.checkNotNull(brokerCtx.config(), "null 'broker config' detected")
                        .brokerName();
        this.logStorage =
                Preconditions.checkNotNull(brokerCtx.logStorage(), "null 'logStorage' detected");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Command command = (Command) msg;

        if (command instanceof PushMessageCommand pushCommand) {
            try (BrokerMdcPropagator notUsed = new BrokerMdcPropagator(brokerName)) {

                LOG.info(
                        "'pushMessage' topic '{}', partition idx '{}', (key = '{}', value = '{}')",
                        pushCommand.topicName(),
                        pushCommand.partitionsIdx(),
                        pushCommand.getMsgKey(),
                        pushCommand.getMsgValue());

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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) {
        ex.printStackTrace();
        ctx.close();
    }
}
