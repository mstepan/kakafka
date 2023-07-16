package com.github.mstepan.kakafka.command;

public record KakafkaCommand(Type type) {

    public static KakafkaCommand metadataCommand() {
        return new KakafkaCommand(Type.GET_METADATA);
    }

    public static KakafkaCommand exitCommand() {
        return new KakafkaCommand(Type.EXIT);
    }

    public enum Type {
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
