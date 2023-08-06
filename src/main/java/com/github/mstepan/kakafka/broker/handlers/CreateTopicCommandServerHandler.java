package com.github.mstepan.kakafka.broker.handlers;

import static com.github.mstepan.kakafka.broker.BrokerMain.BROKER_NAME_MDC_KEY;

import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.Either;
import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CreateTopicCommand;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public final class CreateTopicCommandServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BrokerContext brokerCtx;

    public CreateTopicCommandServerHandler(BrokerContext brokerCtx) {
        this.brokerCtx = brokerCtx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        Command command = (Command) msg;

        if (command instanceof CreateTopicCommand createTopicCommand) {

            try {
                MDC.put(BROKER_NAME_MDC_KEY, brokerCtx.config().brokerName());

                LOG.info(
                        "'create_topic'command for topic '{}' with '{}' partitions",
                        createTopicCommand.topicName(),
                        createTopicCommand.partitionsCount());

                //
                // CreateTopicCommandServerHandler handler is calling 'createTopicInEtcd' method in
                // a
                // blocking fashion,
                // so it will be executed in separate 'EventExecutorGroup' called
                // 'BrokerMain.IO_BLOCKING_OPERATIONS_GROUP'
                //
                Either<TopicInfo> maybeTopicInfo =
                        createTopicInEtcd(brokerCtx.metadata(), createTopicCommand);

                if (maybeTopicInfo.isOk()) {
                    ctx.writeAndFlush(new CreateTopicCommandResponse(maybeTopicInfo.value(), 200));
                } else {
                    ctx.writeAndFlush(new CreateTopicCommandResponse(null, 500));
                }
            } finally {
                MDC.clear();
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

    /**
     * The 'TopicInfo' contains list of 'TopicPartitionInfo' values:
     *
     * <p>[partition-0] [leader=broker-ff5ebc71-ddf5-4f0d-9799-be784640d7c7,
     * replicas=[broker-87f2d83a-b58a-4b90-a441-7077b2713419,
     * broker-26fc73c5-38c2-476a-8a72-65516a91de26]] [partition-1]
     * [leader=broker-7a9af119-d0ba-4aed-8594-19f41b9fe13f,
     * replicas=[broker-8c3598fc-e389-495c-b29c-b5d983afff1e,
     * broker-ff5ebc71-ddf5-4f0d-9799-be784640d7c7]] [partition-2]
     * [leader=broker-87f2d83a-b58a-4b90-a441-7077b2713419,
     * replicas=[broker-26fc73c5-38c2-476a-8a72-65516a91de26,
     * broker-7a9af119-d0ba-4aed-8594-19f41b9fe13f]]
     */
    private Either<TopicInfo> createTopicInEtcd(
            MetadataStorage metadata, CreateTopicCommand command) {
        try {
            List<LiveBroker> brokersSampling =
                    metadata.getSamplingOfLiveBrokers(
                            command.partitionsCount() * command.replicasCnt());

            List<TopicPartitionInfo> partitions = new ArrayList<>();

            Iterator<LiveBroker> samplingIt = brokersSampling.iterator();
            for (int parId = 0; parId < command.partitionsCount(); ++parId) {

                LiveBroker partitionLeader = samplingIt.next();

                partitions.add(
                        new TopicPartitionInfo(
                                parId,
                                partitionLeader.id(),
                                createReplicas(samplingIt, command.replicasCnt() - 1)));
            }

            LOG.info("partitions = '{}'", partitions);

            TopicInfo topicInfo = new TopicInfo(command.topicName(), partitions);

            @SuppressWarnings("resource")
            KV kvClient = brokerCtx.etcdClientHolder().kvClient();

            final ByteSequence topicKey =
                    EtcdUtils.toByteSeq("/kakafka/topics/%s".formatted(command.topicName()));

            //
            // Check if the topic with the same name already exists.
            // If there is a corresponding 'etcd' key, we just throw an exception.
            //
            GetResponse topicGetResponse = kvClient.get(topicKey).get();

            if (topicGetResponse.getCount() != 0L) {
                return Either.error(
                        new IllegalStateException(
                                "Topic with the name '%s' already exists"
                                        .formatted(command.topicName())));
            }

            // store 'TopicInfo' as binary encoded value
            kvClient.put(topicKey, ByteSequence.from(topicInfo.toBytes())).get();

            return Either.ok(topicInfo);
        } catch (Exception ex) {
            ex.printStackTrace();
            return Either.error(ex);
        }
    }

    private List<String> createReplicas(Iterator<LiveBroker> samplingIt, int count) {
        List<String> replicas = new ArrayList<>(count);

        for (int i = 0; i < count; ++i) {
            replicas.add(samplingIt.next().id());
        }
        return replicas;
    }
}
