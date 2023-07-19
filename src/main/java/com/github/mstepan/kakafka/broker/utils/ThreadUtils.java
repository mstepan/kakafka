package com.github.mstepan.kakafka.broker.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Different thread related methods. */
public final class ThreadUtils {

    private ThreadUtils() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static void sleepSec(long delayInSec) {
        sleep(delayInSec, TimeUnit.SECONDS);
    }

    public static void sleepMs(long delayInMs) {
        sleep(delayInMs, TimeUnit.MILLISECONDS);
    }

    public static void sleep(long delay, TimeUnit unit) {
        try {
            unit.sleep(delay);
        } catch (InterruptedException interEx) {
            Thread.currentThread().interrupt();
        }
    }

    public static void waitOn(Object mutex) {
        synchronized (mutex) {
            try {
                mutex.wait();
            } catch (InterruptedException interEx) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void notifyAllOn(Object mutex) {
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }

    public static void decrementBy(AtomicInteger counter, int decValue) {
        while (true) {
            int prevValue = counter.get();
            if (counter.compareAndSet(prevValue, prevValue - decValue)) {
                return;
            }
        }
    }
}
