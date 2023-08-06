package com.github.mstepan.kakafka.broker.handlers;

import static com.github.mstepan.kakafka.broker.BrokerMain.BROKER_NAME_MDC_KEY;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.storage.LogStorage;
import com.github.mstepan.kakafka.broker.utils.Preconditions;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.ConsumeMessageCommand;
import com.github.mstepan.kakafka.command.response.ConsumeMessageCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public final class ConsumeMessageServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String brokerName;
    private final LogStorage logStorage;

    public ConsumeMessageServerHandler(BrokerContext brokerCtx) {
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

        if (command instanceof ConsumeMessageCommand consumeMsgCommand) {

            MDC.put(BROKER_NAME_MDC_KEY, brokerName);

            try {
                LOG.info(
                        "'consumeMessage' command, topic = '{}' partition idx = '{}', offset = '{}'",
                        consumeMsgCommand.topicName(),
                        consumeMsgCommand.partitionsIdx(),
                        consumeMsgCommand.msgIndex());

                StringTopicMessage msgFromTopic =
                        logStorage.getMessage(
                                consumeMsgCommand.topicName(),
                                consumeMsgCommand.partitionsIdx(),
                                consumeMsgCommand.msgIndex());

                // message for specified triplet <topic, partition, offset> wasn't found
                if (msgFromTopic == null) {
                    ctx.writeAndFlush(new ConsumeMessageCommandResponse(null, null, 500));
                } else {
                    ctx.writeAndFlush(
                            new ConsumeMessageCommandResponse(
                                    msgFromTopic.key(), msgFromTopic.value(), 200));
                }
            } finally {
                MDC.clear();
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
