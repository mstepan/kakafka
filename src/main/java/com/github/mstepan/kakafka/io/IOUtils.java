package com.github.mstepan.kakafka.io;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public final class IOUtils {

    private IOUtils() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static void createFolderIfNotExist(String brokerName, Path folderPath) {
        if (Files.notExists(folderPath)) {
            try {
                System.out.printf("[%s]Creating folder '%s'%n", brokerName, folderPath);
                Files.createDirectory(folderPath);
            } catch (IOException ioEx) {
                throw new IllegalStateException(ioEx);
            }
        } else {
            System.out.printf("[%s]Using existing '%s' folder%n", brokerName, folderPath);
        }
    }

    public static void createFileIfNotExist(String brokerName, Path pathToFile) {
        try {
            if (Files.notExists(pathToFile)) {
                System.out.printf("[%s]Creating file '%s'%n", brokerName, pathToFile);
                Files.createFile(
                        pathToFile, PosixFilePermissions.asFileAttribute(fileDefaultPermissions()));
            }
        } catch (IOException ioEx) {
            throw new IllegalStateException(
                    "Can't properly create file '%s'".formatted(pathToFile), ioEx);
        }
    }

    /** Default file permissions: '-rw-r--r--' */
    public static Set<PosixFilePermission> fileDefaultPermissions() {
        return Set.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ);
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
