package com.github.mstepan.kakafka.io;

public final class Preconditions {

    private Preconditions() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static <T> void checkState(boolean condition, String errorMsg) {
        if (!condition) {
            throw new IllegalStateException(errorMsg);
        }
    }

    public static <T> void checkArgument(boolean condition, String errorMsg) {
        if (!condition) {
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
