package com.github.mstepan.kakafka.io;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;

public final class IOUtils {

    private IOUtils() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static void createFolderIfNotExist(String brokerName, Path folderPath) {
        if (Files.notExists(folderPath)) {
            try {
                System.out.printf("[%s]Creating data folder '%s'%n", brokerName, folderPath);
                Files.createDirectory(folderPath);
            } catch (IOException ioEx) {
                throw new IllegalStateException(ioEx);
            }
        } else {
            System.out.printf(
                    "[%s]Using existing '%s' folder as data folder%n", brokerName, folderPath);
        }
    }

    /** Close java socket without throwing IOException. */
    public static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ioEx) {
                throw new IllegalStateException("Can't properly close the Socket", ioEx);
            }
        }
    }
}
