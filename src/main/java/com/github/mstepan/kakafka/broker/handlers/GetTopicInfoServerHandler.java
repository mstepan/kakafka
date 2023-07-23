package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.Either;
import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.GetTopicInfoCommand;
import com.github.mstepan.kakafka.command.response.GetTopicInfoCommandResponse;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public final class GetTopicInfoServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    private final BrokerContext brokerCtx;

    public GetTopicInfoServerHandler(String brokerName, BrokerContext brokerCtx) {
        this.brokerName = brokerName;
        this.brokerCtx = brokerCtx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Command command = (Command) msg;

        if (command instanceof GetTopicInfoCommand topicInfoCommand) {
            try {
                System.out.printf(
                        "[%s] 'get topic info' command received for topic '%s'%n",
                        brokerName, topicInfoCommand.topicName());

                Either<TopicInfo> maybeTopicInfo = getTopicInfo(topicInfoCommand.topicName());

                if (maybeTopicInfo.isOk()) {
                    ctx.writeAndFlush(new GetTopicInfoCommandResponse(maybeTopicInfo.value(), 200));
                } else {
                    ctx.writeAndFlush(new GetTopicInfoCommandResponse(null, 500));
                }
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

    private Either<TopicInfo> getTopicInfo(String topicName) {
        try {
            @SuppressWarnings("resource")
            final KV kvClient = brokerCtx.etcdClientHolder().kvClient();

            final ByteSequence topicKey =
                    EtcdUtils.toByteSeq("/kakafka/topics/%s".formatted(topicName));

            GetResponse getResponse = kvClient.get(topicKey).get();

            if (getResponse.getCount() == 0L) {
                return Either.error(
                        new IllegalArgumentException("Topic '%s' not found".formatted(topicName)));
            }

            KeyValue keyValue = getResponse.getKvs().get(0);

            return Either.ok(TopicInfo.fromBytes(keyValue.getValue().getBytes()));
        } catch (Exception ex) {
            ex.printStackTrace();
            return Either.error(ex);
        }
    }
}
