package com.github.mstepan.kakafka.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;

public class EtcdLeaderElectionMain {

    private static final String ETCD_ENDPOINT = "http://localhost:2379";

    public static void main(String[] args) throws Exception {
        try (Client client = Client.builder().endpoints(ETCD_ENDPOINT).build()) {

            KV kvClient = client.getKVClient();
            ByteSequence key = ByteSequence.from("test_key".getBytes());
            ByteSequence value = ByteSequence.from("test_value".getBytes());

            // put the key-value
//            kvClient.put(key, value).get();

            // delete key
            kvClient.delete(key).get();
        }
    }
}
