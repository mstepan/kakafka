package com.github.mstepan.kakafka.io;

import com.github.mstepan.kakafka.broker.utils.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private IOUtils() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static void createFolderIfNotExist(Path folderPath) {
        if (Files.notExists(folderPath)) {
            LOG.info("Creating folder '{}'", folderPath);
            boolean folderCreated = folderPath.toFile().mkdirs();
            if (!folderCreated) {
                throw new IllegalStateException("Can't create folder '%s'".formatted(folderPath));
            }
        } else {
            LOG.info("Using existing '{}' folder", folderPath);
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

    public static long length(RandomAccessFile file) {
        try {
            return file.length();
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    public static void seek(RandomAccessFile file, long offset) {

        Preconditions.checkArgument(file != null, "null 'file' parameter detected");

        final long fileLength = length(file);

        Preconditions.checkArgument(
                offset >= 0 && offset <= fileLength,
                "incorrect offset value offset = %d, should be in range [%d, %d]"
                        .formatted(offset, 0, fileLength));
        try {
            file.seek(offset);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    /**
     * Delete folder or file identified by 'path' parameter if exists. All intermediate folders and
     * files will be also deleted.
     *
     * <p>Algorithm explained:
     *
     * <p>1. Get all sub-folders and files.
     *
     * <p>2. Sort in reverse older, so that files appear first.
     *
     * <p>3. Delete all files, sub-folders and folders in reverse order.
     */
    public static void delete(String path) {
        try {

            Path pathToDelete = Path.of(path);

            if (!pathToDelete.toFile().exists()) {
                return;
            }

            try (Stream<Path> pathStream = Files.walk(pathToDelete)) {
                pathStream
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(
                                file -> {
                                    boolean wasDeleted = file.delete();
                                    if (!wasDeleted) {
                                        throw new IllegalStateException(
                                                "Can't delete file or folder '%s'".formatted(file));
                                    }
                                });
            }
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }
}
