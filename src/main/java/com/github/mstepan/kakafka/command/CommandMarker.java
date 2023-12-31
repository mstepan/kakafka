package com.github.mstepan.kakafka.command;

public enum CommandMarker {
    CONSUME_MESSAGE(5),

    GET_TOPIC_INFO(4),

    PUSH_MESSAGE(3),

    CREATE_TOPIC(2),

    GET_METADATA(1),
    EXIT(0);

    private final int value;

    public int value() {
        return value;
    }

    CommandMarker(int value) {
        this.value = value;
    }

    public static CommandMarker fromIntValue(int val) {
        for (CommandMarker curType : values()) {
            if (curType.value == val) {
                return curType;
            }
        }
        throw new IllegalStateException(
                "Can't find 'CommandMarker' value using marker '" + val + "'");
    }
}
