package com.github.mstepan.kakafka.broker.core;

public record LiveBroker(String id, String url) {

    public String host() {
        return url.substring(0, url.indexOf(":"));
    }

    public int port() {
        return Integer.parseInt(url.substring(url.indexOf(":") + 1));
    }
}
