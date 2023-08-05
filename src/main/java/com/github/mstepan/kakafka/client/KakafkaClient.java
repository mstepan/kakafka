package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CommandEncoder;
import com.github.mstepan.kakafka.command.ConsumeMessageCommand;
import com.github.mstepan.kakafka.command.CreateTopicCommand;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.GetTopicInfoCommand;
import com.github.mstepan.kakafka.command.PushMessageCommand;
import com.github.mstepan.kakafka.command.response.CommandResponseDecoder;
import com.github.mstepan.kakafka.command.response.ConsumeMessageCommandResponse;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import com.github.mstepan.kakafka.command.response.GetTopicInfoCommandResponse;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import com.github.mstepan.kakafka.command.response.PushMessageCommandResponse;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import com.github.mstepan.kakafka.io.IOUtils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** This class is not thread safe. At least for NOW. So should be instantiated per thread. */
public final class KakafkaClient implements AutoCloseable {

    /** The default number of partitions per topic. */
    public static final int DEFAULT_PARTITIONS_COUNT_PER_TOPIC = 3;

    /**
     * The replication factor for every partition. The number of brokers that will store messages
     * for this partition as replicas.
     */
    public static final int DEFAULT_REPLICATION_FACTOR_FOR_SINGLE_PARTITION = 3;

    // provide list of seed broker to connect to
    private static final List<BrokerHost> SEED_BROKERS =
            List.of(
                    new BrokerHost("localhost", 9091),
                    new BrokerHost("localhost", 9092),
                    new BrokerHost("localhost", 9093),
                    new BrokerHost("localhost", 9094),
                    new BrokerHost("localhost", 9095));

    private Socket clusterLeader;

    private Socket lastBrokerConnection;

    private void checkLastBrokerConnection() {
        if (lastBrokerConnection == null) {
            lastBrokerConnection = findAvailableBroker();
        }
    }

    private void checkClusterLeader() {
        if (clusterLeader == null) {

            Optional<MetadataCommandResponse> metadataResponse = getMetadata();

            if (metadataResponse.isEmpty()) {
                throw new IllegalStateException(
                        "Can't get metadata from cluster to find the cluster leader");
            }

            clusterLeader =
                    connect(
                            BrokerHost.fromLiveBroker(
                                    metadataResponse.get().state().leaderBroker()));
            System.out.println("Successfully connected to LEADER broker");
        }
    }

    /** Get cluster metadata using any alive broker. */
    public Optional<MetadataCommandResponse> getMetadata() {
        checkLastBrokerConnection();

        try {
            DataInputStream dataIn = new DataInputStream(lastBrokerConnection.getInputStream());
            DataOutputStream dataOut = new DataOutputStream(lastBrokerConnection.getOutputStream());

            DataIn in = DataIn.fromStandardStream(dataIn);

            sendCommand(new GetMetadataCommand(), dataOut);

            MetadataCommandResponse metaCommandResp =
                    (MetadataCommandResponse) CommandResponseDecoder.decode(in);

            if (metaCommandResp.statusCode() != 200) {
                System.err.println("Get Metadata request failed");
                return Optional.empty();
            }

            return Optional.of(metaCommandResp);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    /** connect to leader b/c only cluster leader can create new topics */
    public Optional<CreateTopicCommandResponse> createTopic(String topicName) {
        try {
            checkClusterLeader();

            DataInputStream dataIn = new DataInputStream(clusterLeader.getInputStream());
            DataOutputStream dataOut = new DataOutputStream(clusterLeader.getOutputStream());

            sendCommand(
                    new CreateTopicCommand(
                            topicName,
                            DEFAULT_PARTITIONS_COUNT_PER_TOPIC,
                            DEFAULT_REPLICATION_FACTOR_FOR_SINGLE_PARTITION),
                    dataOut);
            CreateTopicCommandResponse createTopicResponse =
                    (CreateTopicCommandResponse)
                            CommandResponseDecoder.decode(DataIn.fromStandardStream(dataIn));

            if (createTopicResponse.status() != 200) {
                System.err.printf("Create topic '%s' FAILED.%n", topicName);
                return Optional.empty();
            }

            System.out.printf("Create topic '%s' success.%n", topicName);

            return Optional.of(createTopicResponse);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    /**
     * Get information for existing topic, such as partitions count, partitions leaders, replicas
     * etc.
     */
    public Optional<GetTopicInfoCommandResponse> getTopicInfo(String topicName) {

        checkLastBrokerConnection();

        try {
            DataInputStream dataIn = new DataInputStream(lastBrokerConnection.getInputStream());
            DataOutputStream dataOut = new DataOutputStream(lastBrokerConnection.getOutputStream());

            sendCommand(new GetTopicInfoCommand(topicName), dataOut);

            GetTopicInfoCommandResponse topicInfoResp =
                    (GetTopicInfoCommandResponse)
                            CommandResponseDecoder.decode(DataIn.fromStandardStream(dataIn));

            if (topicInfoResp.status() != 200) {
                System.err.printf("Topic '%s' not found", topicName);
                return Optional.empty();
            }

            return Optional.of(topicInfoResp);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    /**
     * Find leader broker for topic and partitions tuple. Send push message to the mentioned above
     * broker.
     */
    public Optional<PushMessageCommandResponse> pushMessage(
            String topicName, StringTopicMessage stringTopicMessage) {

        Optional<GetTopicInfoCommandResponse> maybeTopicInfoResp = getTopicInfo(topicName);

        if (maybeTopicInfoResp.isEmpty()) {
            System.err.printf("Can't find topic info for topic '%s'", topicName);
            return Optional.empty();
        }

        List<TopicPartitionInfo> partitions = maybeTopicInfoResp.get().info().partitions();

        int partitionIdx = Math.abs(stringTopicMessage.key().hashCode()) % partitions.size();

        TopicPartitionInfo partitionToPushMessage = partitions.get(partitionIdx);

        Optional<MetadataCommandResponse> maybeMetadata = getMetadata();

        if (maybeMetadata.isEmpty()) {
            System.err.println("Can't get metadata for cluster");
            return Optional.empty();
        }

        LiveBroker topicAndPartitionLeader = getTopicAndPartitionLeader(topicName, partitionIdx);

        BrokerHost brokerHost = BrokerHost.fromLiveBroker(topicAndPartitionLeader);

        try (Socket brokerToPush = connect(brokerHost);
                DataInputStream dataIn = new DataInputStream(brokerToPush.getInputStream());
                DataOutputStream dataOut = new DataOutputStream(brokerToPush.getOutputStream())) {

            DataIn in = DataIn.fromStandardStream(dataIn);

            System.out.printf("Pushing message to broker: %s%n", brokerHost);

            sendCommand(
                    new PushMessageCommand(
                            topicName, partitionToPushMessage.idx(), stringTopicMessage),
                    dataOut);

            PushMessageCommandResponse pushResp =
                    (PushMessageCommandResponse) CommandResponseDecoder.decode(in);

            return Optional.of(pushResp);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    private LiveBroker getTopicAndPartitionLeader(String topicName, int partIdx) {
        Optional<GetTopicInfoCommandResponse> maybeTopicInfoResp = getTopicInfo(topicName);

        if (maybeTopicInfoResp.isEmpty()) {
            throw new IllegalStateException(
                    "Can't find info for topic '%s', so can't obtain leader".formatted(topicName));
        }

        List<TopicPartitionInfo> partitions = maybeTopicInfoResp.get().info().partitions();

        TopicPartitionInfo partitionInfo = partitions.get(partIdx);

        Optional<MetadataCommandResponse> maybeMetadata = getMetadata();

        if (maybeMetadata.isEmpty()) {
            throw new IllegalStateException("Can't obtain cluster wide metadata");
        }

        Optional<LiveBroker> maybeBroker =
                maybeMetadata.get().state().findBrokerById(partitionInfo.leader());

        if (maybeBroker.isEmpty()) {
            throw new IllegalStateException(
                    "Can't find leader for topic '%s' and partition idx '%s'"
                            .formatted(topicName, partIdx));
        }

        return maybeBroker.get();
    }

    /** Consumers read by default from the broker that is the leader for a given partition. */
    public ConsumeMessageCommandResponse consumeMessage(
            String topicName, int partitionIdx, int msgIndex) {

        LiveBroker topicPartitionLeader = getTopicAndPartitionLeader(topicName, partitionIdx);

        BrokerHost brokerHost = BrokerHost.fromLiveBroker(topicPartitionLeader);

        try (Socket brokerToConsume = connect(brokerHost);
                DataInputStream dataIn = new DataInputStream(brokerToConsume.getInputStream());
                DataOutputStream dataOut =
                        new DataOutputStream(brokerToConsume.getOutputStream())) {

            DataIn in = DataIn.fromStandardStream(dataIn);

            sendCommand(new ConsumeMessageCommand(topicName, partitionIdx, msgIndex), dataOut);
            return (ConsumeMessageCommandResponse) CommandResponseDecoder.decode(in);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    private void sendCommand(Command command, DataOutputStream out) throws IOException {
        CommandEncoder.encode(DataOut.fromStandardStream(out), command);
        out.flush();
    }

    private Socket findAvailableBroker() {
        List<BrokerHost> randomOrderedSeedBrokers = new ArrayList<>(SEED_BROKERS);
        Collections.shuffle(randomOrderedSeedBrokers);

        for (BrokerHost curBrokerHost : randomOrderedSeedBrokers) {

            Socket socket = connect(curBrokerHost);

            if (socket != null) {
                System.out.printf(
                        "Initial connection established to '%s:%d'%n",
                        curBrokerHost.host(), curBrokerHost.port());
                return socket;
            }
        }

        System.err.println("All brokers are DOWN!!!");
        throw new IllegalStateException("This line should not be reachable");
    }

    private Socket connect(BrokerHost broker) {
        Socket socket;
        try {
            socket = new Socket(broker.host(), broker.port());
            return socket;
        } catch (IOException ioEx) {
            return null;
        }
    }

    @Override
    public void close() {
        if (clusterLeader != null) {
            IOUtils.closeSocket(clusterLeader);
        }

        if (lastBrokerConnection != null) {
            IOUtils.closeSocket(lastBrokerConnection);
        }
    }
}
