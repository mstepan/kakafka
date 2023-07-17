package com.github.mstepan.kakafka.io;

import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DataIn {

    private ByteBuf nettyBuf;

    private DataInputStream dataInStream;

    private DataIn(ByteBuf nettyBuf) {
        this.nettyBuf = nettyBuf;
    }

    private DataIn(DataInputStream dataInStream) {
        this.dataInStream = dataInStream;
    }

    public static DataIn fromNettyByteBuf(ByteBuf buf) {
        return new DataIn(buf);
    }

    public static DataIn fromStandardStream(DataInputStream in) {
        return new DataIn(in);
    }

    public int readInt() {
        if (nettyBuf != null) {
            return nettyBuf.readInt();
        }
        try {
            return dataInStream.readInt();
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }

    public String readString(int length) {
        if (nettyBuf != null) {
            return nettyBuf.readCharSequence(length, StandardCharsets.US_ASCII).toString();
        }
        try {
            byte[] strData = new byte[length];
            dataInStream.readFully(strData);
            return new String(strData, StandardCharsets.US_ASCII);
        } catch (IOException ioEx) {
            throw new IllegalStateException(ioEx);
        }
    }
}
