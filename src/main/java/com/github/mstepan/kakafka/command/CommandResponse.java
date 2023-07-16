package com.github.mstepan.kakafka.command;

public record CommandResponse(String data) {

    @Override
    public String toString() {
        return data;
    }
}
