package com.github.mstepan.kakafka.broker.utils;

public final class ValidationUtils {

    private ValidationUtils() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static void checkArgument(boolean condition, String errorMsg) {
        if (!condition) {
            throw new IllegalArgumentException(errorMsg);
        }
    }

    public static void checkState(boolean condition, String errorMsg) {
        if (!condition) {
            throw new IllegalStateException(errorMsg);
        }
    }
}
