package com.github.mstepan.kakafka.broker.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class DaemonThreadFactory implements ThreadFactory {

    private final AtomicInteger threadCount = new AtomicInteger(0);

    private final String groupName;

    public DaemonThreadFactory(String groupName) {
        this.groupName = groupName;
    }

    public DaemonThreadFactory() {
        this(null);
    }

    @Override
    public Thread newThread(Runnable action) {
        Thread thread =
                (groupName == null)
                        ? new Thread(action)
                        : new Thread(action, groupName + "-" + threadCount.getAndIncrement());
        thread.setDaemon(true);
        return thread;
    }
}
