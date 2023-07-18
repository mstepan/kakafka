package com.github.mstepan.kakafka.broker.utils;

import io.etcd.jetcd.ByteSequence;
import java.nio.charset.StandardCharsets;

public final class EtcdUtils {

    private EtcdUtils() {
        throw new AssertionError("Can't instantiate utility-only class");
    }

    public static ByteSequence toByteSeq(String str) {
        return ByteSequence.from(str, StandardCharsets.US_ASCII);
    }
}
