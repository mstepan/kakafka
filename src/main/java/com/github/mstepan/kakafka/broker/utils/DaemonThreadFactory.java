package com.github.mstepan.kakafka.broker.utils;

import java.util.concurrent.ThreadFactory;

public final class DaemonThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable action) {
        Thread thread = new Thread(action);
        thread.setDaemon(true);
        return thread;
    }
}
