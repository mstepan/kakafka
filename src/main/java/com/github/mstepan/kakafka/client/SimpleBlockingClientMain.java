package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.CommandEncoder;
import com.github.mstepan.kakafka.command.CommandResponse;
import com.github.mstepan.kakafka.command.CommandResponseDecoder;
import com.github.mstepan.kakafka.command.MetadataCommandResponse;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

public class SimpleBlockingClientMain {

    private static final int NO_AVAILABLE_BROKERS_EXIT_CODE = 3;

    private static final List<BrokerHost> seedBrokers =
            List.of(
                    new BrokerHost("localhost", 9091),
                    new BrokerHost("localhost", 9092),
                    new BrokerHost("localhost", 9093));

    public static void main(String[] args) throws Exception {
        new SimpleBlockingClientMain().run();
    }

    public void run() {

        Socket socket = findAvailableBroker();

        if (socket == null) {
            System.err.println("All brokers are DOWN!!!");
            System.exit(NO_AVAILABLE_BROKERS_EXIT_CODE);
        }

        try (DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

            CommandEncoder.encode(
                    DataOut.fromStandardStream(dataOut), new Command(Command.Type.GET_METADATA));
            dataOut.flush();

            DataIn in = DataIn.fromStandardStream(dataIn);

            CommandResponse response = CommandResponseDecoder.decode(in);

            if (response instanceof MetadataCommandResponse metaCommandResp) {
                MetadataState metaState = metaCommandResp.state();
                System.out.println(metaState.asStr());
            } else {
                System.err.println("Invalid response type");
            }
        } catch (UnknownHostException ex) {
            System.out.println("Server not found: " + ex.getMessage());
        } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
        } finally {
            closeSocket(socket);
        }
    }

    private Socket findAvailableBroker() {
        for (BrokerHost curBrokerHost : seedBrokers) {

            Socket socket = connect(curBrokerHost);

            if (socket != null) {
                return socket;
            }
        }

        return null;
    }

    private Socket connect(BrokerHost broker) {
        Socket socket = null;
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
