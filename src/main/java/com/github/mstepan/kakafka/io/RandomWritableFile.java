package com.github.mstepan.kakafka.io;

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

    public void append(String key, String value) {
        try {
            randomAccessFile.writeBytes(key);
            randomAccessFile.writeBytes(value);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    public void append(long msgId, long msgOffsetInFile) {
        try {
            randomAccessFile.writeLong(msgId);
            randomAccessFile.writeLong(msgOffsetInFile);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }
}
