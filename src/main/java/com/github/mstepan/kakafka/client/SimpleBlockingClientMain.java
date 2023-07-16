package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.command.CommandResponse;
import com.github.mstepan.kakafka.command.KakafkaCommand;
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

            System.out.println("Metadata request sent");

            int marker = dataIn.readInt();

            if (marker == CommandResponse.GET_METADATA_MARKER) {
                System.out.println("Metadat response received");
            }
        } catch (UnknownHostException ex) {
            System.out.println("Server not found: " + ex.getMessage());
        } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
        }
    }
}
