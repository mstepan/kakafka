package com.github.mstepan.kakafka.command;

public record Command(Type type) {

    public static Command metadataCommand() {
        return new Command(Type.GET_METADATA);
    }

    public static Command exitCommand() {
        return new Command(Type.EXIT);
    }

    public enum Type {
        CREATE_TOPIC(2),

        GET_METADATA(1),
        EXIT(0);

        private final int marker;

        public int marker() {
            return marker;
        }

        Type(int marker) {
            this.marker = marker;
        }

        public static Type fromMarker(int val) {
            for (Type curType : values()) {
                if (curType.marker == val) {
                    return curType;
                }
            }
            throw new IllegalStateException(
                    "Can't find KakafkaCommand.Type value using marker '" + val + "'");
        }
    }
}
