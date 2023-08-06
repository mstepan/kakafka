package com.github.mstepan.kakafka.broker.core;

import com.github.mstepan.kakafka.broker.utils.Preconditions;

public record StringTopicMessage(String key, String value) {

    public StringTopicMessage {
        Preconditions.checkNotNull(key, "null 'key' detected");
        Preconditions.checkNotNull(value, "null 'value' detected");
    }
}
