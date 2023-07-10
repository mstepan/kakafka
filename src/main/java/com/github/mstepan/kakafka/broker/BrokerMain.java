package com.github.mstepan.kakafka.broker;

import java.net.InetAddress;

public class BrokerMain {

    private static final int PORT = 9090;

    public static void main(String[] args) throws Exception {
        System.out.printf(
                "Broker started at %s:%d\n", InetAddress.getLocalHost().getHostName(), PORT);
    }
}
