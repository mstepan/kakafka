package com.github.mstepan.kakafka.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class EtcdLeaderElectionMain {

    private static final String ETCD_ENDPOINT = "http://localhost:2379";

    private static final ByteSequence LEADER_KEY =
            ByteSequence.from("/kakafka/leader", StandardCharsets.UTF_8);

    private static final long LEASE_TTL_IN_SEC = 3L;

    public static void main(String[] args) throws Exception {

        final String brokerId = getBrokerId();

        System.out.printf("'%s' started%n", brokerId);

        try (Client client = Client.builder().endpoints(ETCD_ENDPOINT).build();
                Lease lease = client.getLeaseClient();
                Election electionClient = client.getElectionClient()) {

            LeaseGrantResponse leaseResp = lease.grant(LEASE_TTL_IN_SEC).get();

            ByteSequence serverId = ByteSequence.from(brokerId, StandardCharsets.UTF_8);

            electionClient.observe(
                    LEADER_KEY,
                    new Election.Listener() {
                        @Override
                        public void onNext(LeaderResponse leaderResponse) {
                            System.out.printf(
                                    "Leader selected: %s%n", leaderResponse.getKv().getValue());
                        }

                        @Override
                        public void onError(Throwable ex) {
                            ex.printStackTrace();
                        }

                        @Override
                        public void onCompleted() {
                            System.out.println("Election done");
                        }
                    });

            electionClient.campaign(LEADER_KEY, leaseResp.getID(), serverId);

            while (true) {
                TimeUnit.SECONDS.sleep(1);
                lease.keepAliveOnce(leaseResp.getID()).get();
            }
        }
    }

    private static String getBrokerId() {
        return System.getProperty("broker.id");
    }

    private static void addKeyValue(Client client, String key, String value) throws Exception {
        try (KV kvClient = client.getKVClient()) {
            ByteSequence keyBytes = ByteSequence.from(key.getBytes());
            ByteSequence valueBytes = ByteSequence.from(value.getBytes());

            // put the key-value
            kvClient.put(keyBytes, valueBytes).get();
        }
    }

    private static void deleteKey(Client client, String key) throws Exception {
        try (KV kvClient = client.getKVClient()) {
            ByteSequence keyBytes = ByteSequence.from(key.getBytes());

            // delete key
            kvClient.delete(keyBytes).get();
        }
    }
}
