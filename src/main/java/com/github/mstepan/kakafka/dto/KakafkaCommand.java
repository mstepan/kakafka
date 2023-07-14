package com.github.mstepan.kakafka.dto;

public class KakafkaCommand {

    private final Type type;

    public KakafkaCommand(Type type) {
        this.type = type;
    }

    public Type type() {
        return type;
    }

    public enum Type {
        GET_METADATA(1),
        EXIT(0);

        private final int marker;

        public int marker(){
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
