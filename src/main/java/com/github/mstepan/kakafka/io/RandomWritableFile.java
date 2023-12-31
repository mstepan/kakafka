package com.github.mstepan.kakafka.io;

import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.storage.MessageStreamStatus;
import com.github.mstepan.kakafka.broker.utils.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

/*
Wrapper class that opens java 'RandomAccessFile' in a write mode and move file cursor to the end of a file, so that it's
ready for append operations.
 */
public final class RandomWritableFile {

    private final File originalFile;
    private final RandomAccessFile randomAccessFile;

    /** Track file length explicitly, instead of calling 'randomAccessFile.length()' every time. */
    private long length;

    public RandomWritableFile(Path filePath) {
        Preconditions.checkArgument(filePath != null, "null 'filePath' detected");
        try {
            this.originalFile = filePath.toFile();
            this.randomAccessFile = new RandomAccessFile(originalFile, "rw");
            this.length = randomAccessFile.length();
            randomAccessFile.seek(length);
        } catch (IOException ioEx) {
            throw new ExceptionInInitializerError(ioEx);
        }
    }

    public long appendKeyAndValue(String key, String value) {
        try {
            randomAccessFile.writeInt(key.length());
            length += Integer.BYTES;

            randomAccessFile.writeBytes(key);
            length += key.length();

            randomAccessFile.writeInt(value.length());
            length += Integer.BYTES;

            randomAccessFile.writeBytes(value);
            length += value.length();

            return length;
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    public long appendMessageOffset(int msgId, long msgOffsetInFile) {
        try {
            randomAccessFile.writeInt(msgId);
            length += Integer.BYTES;

            randomAccessFile.writeLong(msgOffsetInFile);
            length += Long.BYTES;

            System.out.printf("message idx %d, file offset %d%n", msgId, msgOffsetInFile);

            return length;
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    // sizeof(msgId INT) + sizeof(msgOffset LONG)
    private static final long IDX_AND_FILE_OFFSET_SIZE_IN_BYTES = Integer.BYTES + Long.BYTES;

    public MessageStreamStatus readLastMessageIndexAndOffset() {
        try {
            if (length == 0L) {
                return null;
            }

            randomAccessFile.seek(length - IDX_AND_FILE_OFFSET_SIZE_IN_BYTES);

            int msgIdx = randomAccessFile.readInt();
            long fileOffset = randomAccessFile.readLong();
            return new MessageStreamStatus(msgIdx, fileOffset);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    public void moveToStart() {
        IOUtils.seek(randomAccessFile, 0L);
    }

    public void moveToEnd() {
        IOUtils.seek(randomAccessFile, IOUtils.length(randomAccessFile));
    }

    public MessageStreamStatus findMessageOffset(int msgIdx) {
        try {
            long fileOffset = 0L;

            while (fileOffset < IOUtils.length(randomAccessFile)) {
                int curMsgIdx = randomAccessFile.readInt();
                long curOffset = randomAccessFile.readLong();

                // move offset forward according to consumed bytes = sizeof(int) + sizeof(long)
                fileOffset += (Integer.BYTES + Long.BYTES);

                if (curMsgIdx == msgIdx) {
                    return new MessageStreamStatus(msgIdx, curOffset);
                }
            }

        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }

        return null;
    }

    public StringTopicMessage readMessageByOffset(long offset) {
        IOUtils.seek(randomAccessFile, offset);

        try {
            int keyLength = randomAccessFile.readInt();
            byte[] keyData = new byte[keyLength];
            randomAccessFile.readFully(keyData);

            int valueLength = randomAccessFile.readInt();
            byte[] valueData = new byte[valueLength];
            randomAccessFile.readFully(valueData);

            return new StringTopicMessage(new String(keyData), new String(valueData));

        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    @Override
    public String toString() {
        return randomAccessFile == null ? "<null>" : originalFile.toString();
    }

    public void close() {
        try {
            randomAccessFile.close();
            System.out.printf("File '%s' closed%n", originalFile.getAbsolutePath());
        } catch (IOException ioEx) {
            throw new IllegalStateException(
                    "Can't properly close the file '%s'".formatted(originalFile.getAbsoluteFile()),
                    ioEx);
        }
    }
}
