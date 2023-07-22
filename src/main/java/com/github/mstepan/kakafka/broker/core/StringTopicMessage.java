package com.github.mstepan.kakafka.broker.core;

import java.util.Objects;

public record StringTopicMessage(String key, String value) {

    public StringTopicMessage {
        Objects.requireNonNull(key, "null 'key' detected");
        Objects.requireNonNull(value, "null 'value' detected");
    }
}
