package com.github.mstepan.kakafka.io;

import io.netty.buffer.ByteBuf;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DataOut {

    private ByteBuf nettyBuf;

    private DataOutputStream dataOutStream;

    private DataOut(ByteBuf nettyBuf) {
        this.nettyBuf = nettyBuf;
    }

    private DataOut(DataOutputStream dataOutStream) {
        this.dataOutStream = dataOutStream;
    }

    public static DataOut fromNettyByteBuf(ByteBuf buf) {
        return new DataOut(buf);
    }

    public static DataOut fromStandardStream(DataOutputStream out) {
        return new DataOut(out);
    }

    public void writeInt(int value) {
        if (nettyBuf != null) {
            nettyBuf.writeInt(value);
        } else {
            try {
                dataOutStream.writeInt(value);
            } catch (IOException ioEx) {
                throw new IllegalStateException(ioEx);
            }
        }
    }

    public void writeString(String value) {
        if (nettyBuf != null) {
            nettyBuf.writeInt(value.length());
            nettyBuf.writeCharSequence(value, StandardCharsets.US_ASCII);
        } else {
            try {
                dataOutStream.writeInt(value.length());
                dataOutStream.writeBytes(value);
            } catch (IOException ioEx) {
                throw new IllegalStateException(ioEx);
            }
        }
    }
}
