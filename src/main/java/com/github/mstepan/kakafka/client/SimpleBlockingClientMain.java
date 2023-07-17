package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.command.CommandResponse;
import com.github.mstepan.kakafka.command.CommandResponseDecoder;
import com.github.mstepan.kakafka.command.GetMetadataResponse;
import com.github.mstepan.kakafka.command.KakafkaCommand;
import com.github.mstepan.kakafka.io.DataIn;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class SimpleBlockingClientMain {

    private static final String HOST = "localhost";

    private static final int PORT = 9091;

    public static void main(String[] args) throws Exception {
        new SimpleBlockingClientMain().run(HOST, PORT);
    }

    public void run(String host, int port) {

        try (Socket socket = new Socket(host, port);
                DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

            dataOut.writeInt(KakafkaCommand.Type.GET_METADATA.marker());
            dataOut.flush();

            DataIn in = DataIn.fromStandardStream(dataIn);

            CommandResponse response = CommandResponseDecoder.decode(in);

            if (response instanceof GetMetadataResponse metaCommandResp) {

                MetadataState metaState = metaCommandResp.state();

                System.out.printf(
                        "leader name:%s, url: %s %n",
                        metaState.leaderBrokerName(), metaState.leaderUrl());

                for (LiveBroker broker : metaState.brokers()) {
                    System.out.printf("id: %s, url: %s %n", broker.id(), broker.url());
                }
            } else {
                System.err.println("Invalid response type");
            }
        } catch (UnknownHostException ex) {
            System.out.println("Server not found: " + ex.getMessage());
        } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
        }
    }
}
