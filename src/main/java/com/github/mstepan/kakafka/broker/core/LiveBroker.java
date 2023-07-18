package com.github.mstepan.kakafka.broker.core;

public record LiveBroker(String id, String url) {

    public String host() {
        return url.substring(0, url.indexOf(":"));
    }

    public int port() {
        return Integer.parseInt(url.substring(url.indexOf(":") + 1));
    }

    @Override
    public String toString() {
        return "id: '%s', url: '%s'".formatted(id, url);
    }
}
