package com.github.mstepan.kakafka.broker.core;

import java.util.Objects;

/** Container for valid value or Exception otherwise. */
public final class Either<T> {

    final T value;
    final Exception ex;

    private Either(T value, Exception ex) {
        this.value = value;
        this.ex = ex;
    }

    public static <T> Either<T> error(Exception ex) {
        Objects.requireNonNull(ex, "null 'ex' for Either detected");
        return new Either<>(null, ex);
    }

    public static <T> Either<T> ok(T value) {
        Objects.requireNonNull(value, "null 'value' for Either detected");
        return new Either<>(value, null);
    }

    public boolean isError() {
        return ex != null;
    }

    public boolean isOk() {
        return value != null;
    }

    public T value() {
        if (isError()) {
            throw new IllegalStateException("Can't obtain value from Either, it is in ERROR state");
        }
        return value;
    }

    public Exception exception() {
        if (isOk()) {
            throw new IllegalStateException(
                    "Can't obtain 'exception' from Either, it is in OK state");
        }
        return ex;
    }
}
