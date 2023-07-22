package com.github.mstepan.kakafka.io;

import java.io.IOException;
import java.net.Socket;

public final class IOUtils {

    private IOUtils() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    /** Close java socket without throwing IOException. */
    public void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ioEx) {
                throw new IllegalStateException("Can't properly close the Socket", ioEx);
            }
        }
    }
}
