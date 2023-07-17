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
        }
    }
}
