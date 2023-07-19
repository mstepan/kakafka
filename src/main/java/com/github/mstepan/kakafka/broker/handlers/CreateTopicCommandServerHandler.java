package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CreateTopicCommand;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class CreateTopicCommandServerHandler extends ChannelInboundHandlerAdapter {

    private final BrokerContext brokerCtx;

    public CreateTopicCommandServerHandler(BrokerContext brokerCtx) {
        this.brokerCtx = brokerCtx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        final String brokerName = brokerCtx.config().brokerName();

        Command command = (Command) msg;

        if (command instanceof CreateTopicCommand createTopicCommand) {
            try {
                System.out.printf(
                        "[%s] 'create_topic'command for topic '%s' with '%d' partitions %n",
                        brokerName,
                        createTopicCommand.topicName(),
                        createTopicCommand.partitionsCount());

                TopicInfo info = createTopicInEtcd(brokerCtx.metadata(), createTopicCommand);

                ctx.writeAndFlush(new CreateTopicCommandResponse(info, 200));
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

    private static final int REPLICATION_FACTOR = 3;

    private TopicInfo createTopicInEtcd(MetadataStorage metadata, CreateTopicCommand command) {

        List<LiveBroker> brokersSampling =
                metadata.getSamplingOfLiveBrokers(command.partitionsCount() * REPLICATION_FACTOR);

        List<TopicPartitionInfo> partitions = new ArrayList<>();

        Iterator<LiveBroker> samplingIt = brokersSampling.iterator();
        for (int parId = 0; parId < command.partitionsCount(); ++parId) {

            LiveBroker partitionLeader = samplingIt.next();

            partitions.add(
                    new TopicPartitionInfo(
                            partitionLeader.id(),
                            createReplicas(samplingIt, REPLICATION_FACTOR - 1)));
        }

        System.out.printf("[%s] partitions = '%s'%n", brokerCtx.config().brokerName(), partitions);

        // todo: save topic info with partitions in 'etcd'

        return new TopicInfo(partitions);
    }

    private List<String> createReplicas(Iterator<LiveBroker> samplingIt, int count) {
        List<String> replicas = new ArrayList<>(count);

        for (int i = 0; i < count; ++i) {
            replicas.add(samplingIt.next().id());
        }
        return replicas;
    }
}
