package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CommandEncoder;
import com.github.mstepan.kakafka.command.CreateTopicCommand;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.response.CommandResponse;
import com.github.mstepan.kakafka.command.response.CommandResponseDecoder;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class SimpleBlockingClientMain {

    private static final int NO_AVAILABLE_BROKERS_EXIT_CODE = 3;
    private static final int CANT_CONNECT_TO_LEADER_EXIT_CODE = 4;

    // provide list of seed broker to connect to
    private static final List<BrokerHost> seedBrokers =
            List.of(
                    new BrokerHost("localhost", 9091),
                    new BrokerHost("localhost", 9092),
                    new BrokerHost("localhost", 9093),
                    new BrokerHost("localhost", 9094),
                    new BrokerHost("localhost", 9095));

    public static void main(String[] args) throws Exception {
//        for (int i = 0; i < 100_000; ++i) {
            new SimpleBlockingClientMain().run();
//        }
    }

    public void run() throws IOException {

        Socket socket = findAvailableBroker();

        if (socket == null) {
            System.err.println("All brokers are DOWN!!!");
            System.exit(NO_AVAILABLE_BROKERS_EXIT_CODE);
        }
        try {
            MetadataState metaState = getMetadata(socket);
            System.out.println(metaState.asStr());

            Socket leader =
                    connect(
                            new BrokerHost(
                                    metaState.leaderBroker().host(),
                                    metaState.leaderBroker().port()));

            try {
                if (leader == null) {
                    System.exit(CANT_CONNECT_TO_LEADER_EXIT_CODE);
                }

                System.out.println("Successfully connected to LEADER broker");

                try (DataInputStream dataIn = new DataInputStream(leader.getInputStream());
                        DataOutputStream dataOut = new DataOutputStream(leader.getOutputStream())) {
                    sendCommand(new CreateTopicCommand("topic-a", 3), dataOut);
                    CommandResponse response =
                            CommandResponseDecoder.decode(DataIn.fromStandardStream(dataIn));
                }

            } finally {
                closeSocket(leader);
            }
        } finally {
            closeSocket(socket);
        }
    }

    private void sendCommand(Command command, DataOutputStream out) throws IOException {
        CommandEncoder.encode(DataOut.fromStandardStream(out), command);
        out.flush();
    }

    private MetadataState getMetadata(Socket socket) throws IOException {
        try (DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

            DataIn in = DataIn.fromStandardStream(dataIn);

            sendCommand(new GetMetadataCommand(), dataOut);

            CommandResponse response = CommandResponseDecoder.decode(in);

            if (response instanceof MetadataCommandResponse metaCommandResp) {

                if (metaCommandResp.statusCode() == 200) {
                    return metaCommandResp.state();
                } else {
                    throw new IllegalStateException("Get Metadata failed");
                }
            } else {
                throw new IllegalStateException("Can't obtain metadata from broker.");
            }
        }
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

        return null;
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

    private void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ioEx) {
                throw new IllegalStateException("Can't properly close the Socket", ioEx);
            }
        }
    }
}
