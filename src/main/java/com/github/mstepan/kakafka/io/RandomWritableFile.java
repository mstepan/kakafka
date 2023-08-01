package com.github.mstepan.kakafka.io;

import com.github.mstepan.kakafka.broker.wal.MessageIndexAndOffset;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

/*
Wrapper class that opens java 'RandomAccessFile' in a write mode and move file cursor to the end of a file, so that it's
ready for append operations.
 */
public final class RandomWritableFile {

    private final RandomAccessFile randomAccessFile;

    public RandomWritableFile(Path filePath) {
        try {
            this.randomAccessFile = new RandomAccessFile(filePath.toFile(), "rw");
            randomAccessFile.seek(randomAccessFile.length());
        } catch (IOException ioEx) {
            throw new ExceptionInInitializerError(ioEx);
        }
    }

    public long end() {
        try {
            return randomAccessFile.length();
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    public long appendKeyValue(String key, String value) {
        try {
            randomAccessFile.writeBytes(key);
            randomAccessFile.writeBytes(value);
            return randomAccessFile.length();
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    public long appendMessageOffset(long msgId, long msgOffsetInFile) {
        try {
            randomAccessFile.writeLong(msgId);
            randomAccessFile.writeLong(msgOffsetInFile);

            System.out.printf("message idx %d, file offset %d%n", msgId, msgOffsetInFile);

            return randomAccessFile.length();
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    private static final long IDX_AND_FILE_OFFSET_SIZE_IN_BYTES = 2 * Long.BYTES;

    public MessageIndexAndOffset readLastMessageIndexAndOffset() {
        try {
            if (randomAccessFile.length() == 0L) {
                return null;
            }

            randomAccessFile.seek(randomAccessFile.length() - IDX_AND_FILE_OFFSET_SIZE_IN_BYTES);

            long msgIdx = randomAccessFile.readLong();
            long fileOffset = randomAccessFile.readLong();
            return new MessageIndexAndOffset(msgIdx, fileOffset);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }
}
