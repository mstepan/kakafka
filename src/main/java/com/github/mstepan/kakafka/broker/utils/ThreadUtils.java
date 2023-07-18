package com.github.mstepan.kakafka.broker.utils;

import java.util.concurrent.TimeUnit;

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
}
