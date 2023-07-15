package com.github.mstepan.kakafka.command;

public class CommandResponse {

    private final String data;

    public CommandResponse(String data) {
        this.data = data;
    }

    public String data() {
        return data;
    }

    @Override
    public String toString() {
        return data;
    }
}
