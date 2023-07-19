package com.github.mstepan.kakafka.client;

import java.util.concurrent.SynchronousQueue;

public final class SynchChannel<T> {

    private final SynchronousQueue<T> syncState = new SynchronousQueue<>();

    public void set(T value) {
        syncState.add(value);
    }

    public T get() {
        try {
            return syncState.take();
        } catch (InterruptedException interEx) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
