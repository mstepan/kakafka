package com.github.mstepan.kakafka.io;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
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

    /** Create file and all intermediate folder as needed if not exist. */
    public static void createFileIfNotExist(Path pathToFile) {
        try {
            File file = pathToFile.toFile();

            if (!file.exists()) {
                File parentFolder = file.getParentFile();

                // check parent folder exist, if not create all intermediate folders
                if (!parentFolder.exists()) {
                    boolean intermediateDirsCreated = parentFolder.mkdirs();
                    if (!intermediateDirsCreated) {
                        throw new IllegalStateException(
                                "Can't create intermediate directories '%s'"
                                        .formatted(parentFolder));
                    }
                }

                // create file after all intermediate folder constructed
                boolean fileCreated = file.createNewFile();
                if (!fileCreated) {
                    throw new IllegalStateException("Can't create file '%s'".formatted(file));
                }
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

    /** Check if specified file or folder exist. */
    public static boolean exist(Path path) {
        return Files.exists(path);
    }
}
