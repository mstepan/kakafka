package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CommandEncoder;
import com.github.mstepan.kakafka.command.CreateTopicCommand;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.GetTopicInfoCommand;
import com.github.mstepan.kakafka.command.PushMessageCommand;
import com.github.mstepan.kakafka.command.response.CommandResponseDecoder;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import com.github.mstepan.kakafka.command.response.GetTopicInfoCommandResponse;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import com.github.mstepan.kakafka.command.response.PushMessageCommandResponse;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class KakafkaClient {

    // provide list of seed broker to connect to
    private static final List<BrokerHost> SEED_BROKERS =
            List.of(
                    new BrokerHost("localhost", 9091),
                    new BrokerHost("localhost", 9092),
                    new BrokerHost("localhost", 9093),
                    new BrokerHost("localhost", 9094),
                    new BrokerHost("localhost", 9095));

    /** Get cluster metadata using any alive broker. */
    public Optional<MetadataCommandResponse> getMetadata() {

        try (Socket socket = findAvailableBroker();
                DataInputStream dataIn = new DataInputStream(socket.getInputStream());
                DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream())) {

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
    public Optional<TopicInfo> createTopic(String topicName) {

        Optional<MetadataCommandResponse> metadataResponse = getMetadata();

        if (metadataResponse.isEmpty()) {
            System.err.println("Can't get metadata from cluster");
            return Optional.empty();
        }

        try (Socket leader =
                connect(BrokerHost.fromLiveBroker(metadataResponse.get().state().leaderBroker()))) {
            if (leader == null) {
                System.err.println("Can't connect to LEADER broker");
                return Optional.empty();
            }

            System.out.println("Successfully connected to LEADER broker");

            try (DataInputStream dataIn = new DataInputStream(leader.getInputStream());
                    DataOutputStream dataOut = new DataOutputStream(leader.getOutputStream())) {

                final int partitionsCnt = 3;
                final int replicasCnt = 3;

                sendCommand(new CreateTopicCommand(topicName, partitionsCnt, replicasCnt), dataOut);
                CreateTopicCommandResponse createTopicResponse =
                        (CreateTopicCommandResponse)
                                CommandResponseDecoder.decode(DataIn.fromStandardStream(dataIn));

                if (createTopicResponse.status() != 200) {
                    System.err.printf("Create topic '%s' FAILED.%n", topicName);
                    return Optional.empty();
                }

                System.out.printf("Create topic '%s' success.%n", topicName);

                return Optional.of(createTopicResponse.info());
            }
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    /**
     * Get information for existing topic, such as partitions count, partitions leaders, replicas
     * etc.
     */
    public Optional<TopicInfo> getTopicInfo(String topicName) {
        try (Socket socket = findAvailableBroker();
                DataInputStream dataIn = new DataInputStream(socket.getInputStream());
                DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream())) {

            sendCommand(new GetTopicInfoCommand(topicName), dataOut);

            GetTopicInfoCommandResponse topicInfoResp =
                    (GetTopicInfoCommandResponse)
                            CommandResponseDecoder.decode(DataIn.fromStandardStream(dataIn));

            if (topicInfoResp.status() != 200) {
                System.err.printf("Topic '%s' not found", topicName);
                return Optional.empty();
            }

            return Optional.of(topicInfoResp.info());
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

        Optional<TopicInfo> maybeTopicInfo = getTopicInfo(topicName);

        if (maybeTopicInfo.isEmpty()) {
            System.err.printf("Can't find topic info for topic '%s'", topicName);
            return Optional.empty();
        }

        List<TopicPartitionInfo> partitions = maybeTopicInfo.get().partitions();

        int partitionIdx = Math.abs(stringTopicMessage.key().hashCode()) % partitions.size();

        TopicPartitionInfo partitionToPushMessage = partitions.get(partitionIdx);

        Optional<MetadataCommandResponse> maybeMetadata = getMetadata();

        if (maybeMetadata.isEmpty()) {
            System.err.println("Can't get metadata for cluster");
            return Optional.empty();
        }

        Optional<LiveBroker> maybeBroker =
                maybeMetadata.get().state().findBrokerById(partitionToPushMessage.leader());

        if (maybeBroker.isEmpty()) {
            System.err.printf(
                    "Can't find leader broker for topic '%s' and partition '%d' to push message",
                    topicName, partitionToPushMessage.idx());
            return Optional.empty();
        }

        BrokerHost brokerHost = maybeBroker.map(BrokerHost::fromLiveBroker).get();

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
}
