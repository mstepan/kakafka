package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CommandEncoder;
import com.github.mstepan.kakafka.command.CreateTopicCommand;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.PushMessageCommand;
import com.github.mstepan.kakafka.command.response.CommandResponseDecoder;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
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
import java.util.UUID;

public final class SimpleBlockingClientMain {

    private static final int NO_AVAILABLE_BROKERS_EXIT_CODE = 3;

    private static final int CANT_CONNECT_TO_LEADER_EXIT_CODE = 4;
    private static final int GET_METADATA_FAILED_EXIT_CODE = 5;

    private static final int CANT_CREATE_TOPIC_CODE = 6;

    private static final int CANT_FIND_TOPIC_PARTITION_LEADER_EXIT_CODE = 7;

    // provide list of seed broker to connect to
    private static final List<BrokerHost> seedBrokers =
            List.of(
                    new BrokerHost("localhost", 9091),
                    new BrokerHost("localhost", 9092),
                    new BrokerHost("localhost", 9093),
                    new BrokerHost("localhost", 9094),
                    new BrokerHost("localhost", 9095));

    public static void main(String[] args) throws Exception {

        final int iterationsCount = 1;
        final int clientsCount = 1;
        final Thread[] clients = new Thread[clientsCount];

        for (int i = 0; i < clients.length; ++i) {
            clients[i] =
                    new Thread(
                            () -> {
                                try {
                                    for (int it = 0; it < iterationsCount; ++it) {
                                        new SimpleBlockingClientMain().run();
                                        //
                                        // TimeUnit.SECONDS.sleep(1L);
                                    }
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            });
        }
        for (Thread curClientTh : clients) {
            curClientTh.start();
        }

        for (Thread curClientTh : clients) {
            curClientTh.join();
        }
    }

    public void run() {

        try (Socket socket = findAvailableBroker()) {

            final MetadataCommandResponse metaResponse = getMetadata(socket);
            final MetadataState metaState = metaResponse.state();
            printMetadata(metaState);

            try (Socket leader = connect(BrokerHost.fromLiveBroker(metaState.leaderBroker()))) {
                if (leader == null) {
                    System.exit(CANT_CONNECT_TO_LEADER_EXIT_CODE);
                }

                System.out.println("Successfully connected to LEADER broker");

                TopicInfo info = createTopic(leader);
                printTopicInfo(info);

                pushMessage(info, metaState, new StringTopicMessage("some-key-value-123345466", "hello world!!!"));
            }
        } catch (IOException ioEx) {
            ioEx.printStackTrace();
        }
    }

    private MetadataCommandResponse getMetadata(Socket socket) throws IOException {
        try (DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

            DataIn in = DataIn.fromStandardStream(dataIn);

            sendCommand(new GetMetadataCommand(), dataOut);

            MetadataCommandResponse metaCommandResp =
                    (MetadataCommandResponse) CommandResponseDecoder.decode(in);

            if (metaCommandResp.statusCode() != 200) {
                System.err.println("Get Metadata request failed");
                System.exit(GET_METADATA_FAILED_EXIT_CODE);
            }

            return metaCommandResp;
        }
    }

    private void printMetadata(MetadataState metaState) {
        System.out.println(metaState.asStr());
    }

    private TopicInfo createTopic(Socket leader) throws IOException {
        try (DataInputStream dataIn = new DataInputStream(leader.getInputStream());
                DataOutputStream dataOut = new DataOutputStream(leader.getOutputStream())) {

            final String topicName = "topic-" + UUID.randomUUID();
            final int partitionsCnt = 3;
            final int replicasCnt = 3;

            sendCommand(new CreateTopicCommand(topicName, partitionsCnt, replicasCnt), dataOut);
            CreateTopicCommandResponse createTopicResponse =
                    (CreateTopicCommandResponse)
                            CommandResponseDecoder.decode(DataIn.fromStandardStream(dataIn));

            if (createTopicResponse.status() != 200) {
                System.err.printf("Create topic '%s' FAILED.%n", topicName);
                System.exit(CANT_CREATE_TOPIC_CODE);
            }

            System.out.printf("Create topic '%s' success.%n", topicName);

            return createTopicResponse.info();
        }
    }

    private void printTopicInfo(TopicInfo info) {
        System.out.printf("%nTOPIC INFO%n");
        System.out.printf("topic: %s%n", info.topicName());
        for (TopicPartitionInfo partitionInfo : info.partitions()) {
            System.out.printf("[partition-%d]: %s%n", partitionInfo.idx(), partitionInfo);
        }
    }

    private void pushMessage(
            TopicInfo info, MetadataState metaState, StringTopicMessage stringTopicMessage)
            throws IOException {

        List<TopicPartitionInfo> partitions = info.partitions();

        int partitionIdx = Math.abs(stringTopicMessage.key().hashCode()) % partitions.size();

        TopicPartitionInfo partitionToPushMessage = partitions.get(partitionIdx);

        Optional<LiveBroker> maybeBroker =
                metaState.findBrokerById(partitionToPushMessage.leader());

        if (maybeBroker.isEmpty()) {
            System.exit(CANT_FIND_TOPIC_PARTITION_LEADER_EXIT_CODE);
        }

        BrokerHost brokerHost = maybeBroker.map(BrokerHost::fromLiveBroker).get();

        try (Socket brokerToPush = connect(brokerHost);
             DataInputStream dataIn = new DataInputStream(brokerToPush.getInputStream());
             DataOutputStream dataOut = new DataOutputStream(brokerToPush.getOutputStream())) {

            DataIn in = DataIn.fromStandardStream(dataIn);

            System.out.printf("Pushing message to broker: %s%n", brokerHost);

            sendCommand(
                    new PushMessageCommand(
                            info.topicName(), partitionToPushMessage.idx(), stringTopicMessage),
                    dataOut);

            PushMessageCommandResponse pushResp =
                    (PushMessageCommandResponse) CommandResponseDecoder.decode(in);
        }
    }

    private void sendCommand(Command command, DataOutputStream out) throws IOException {
        CommandEncoder.encode(DataOut.fromStandardStream(out), command);
        out.flush();
    }

    private Socket findAvailableBroker() {
        List<BrokerHost> randomOrderedSeedBrokers = new ArrayList<>(seedBrokers);
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
        System.exit(NO_AVAILABLE_BROKERS_EXIT_CODE);
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
