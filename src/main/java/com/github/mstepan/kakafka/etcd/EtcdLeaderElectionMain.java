package com.github.mstepan.kakafka.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EtcdLeaderElectionMain {

    private static final String ETCD_ENDPOINT = "http://localhost:2379";

    private static final ByteSequence LEADER_KEY =
            ByteSequence.from("/kakafka/leader", StandardCharsets.UTF_8);

    private static final ByteSequence BROKER_ID_LOCK_KEY =
            ByteSequence.from("/kakafka/broker/key/lock", StandardCharsets.UTF_8);

    private static final ByteSequence LAST_BROKER_ID_KEY =
            ByteSequence.from("/kakafka/broker/key/last", StandardCharsets.UTF_8);

    private static final long LEASE_TTL_IN_SEC = 3L;

    public static void main(String[] args) throws Exception {

        try (Client client = Client.builder().endpoints(ETCD_ENDPOINT).build();
                Lease lease = client.getLeaseClient();
                Election electionClient = client.getElectionClient()) {

            LeaseGrantResponse leaseResp =
                    lease.grant(LEASE_TTL_IN_SEC, 3L, TimeUnit.SECONDS).get();

            final String brokerId = generateUniqueBrokerName(client, lease);

            System.out.printf("'%s' started%n", brokerId);

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

            // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Election.java
            electionClient.campaign(LEADER_KEY, leaseResp.getID(), serverId);

            while (true) {
                TimeUnit.SECONDS.sleep(1);
                // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Lease.java
                lease.keepAliveOnce(leaseResp.getID()).get();
            }
        }
    }

    private static String generateUniqueBrokerName(Client client, Lease lease) throws Exception {

        try (Lock lock = client.getLockClient()) {

            LeaseGrantResponse leaseResp =
                    lease.grant(LEASE_TTL_IN_SEC, 3L, TimeUnit.SECONDS).get();

            lock.lock(BROKER_ID_LOCK_KEY, leaseResp.getID()).get();
            try {
                try (KV kv = client.getKVClient()) {
                    GetResponse keyResp = kv.get(LAST_BROKER_ID_KEY).get();

                    if (keyResp.getKvs().isEmpty()) {
                        kv.put(LAST_BROKER_ID_KEY, ByteSequence.from("1", StandardCharsets.UTF_8))
                                .get();
                        return "broker-0";
                    } else {
                        List<KeyValue> data = keyResp.getKvs();

                        assert data.size() > 0 : "List<KeyValue> data is empty";

                        String curValAsStr =
                                data.get(0).getValue().toString(StandardCharsets.UTF_8);

                        int curVal = Integer.parseInt(curValAsStr.trim());

                        kv.put(
                                        LAST_BROKER_ID_KEY,
                                        ByteSequence.from(
                                                String.valueOf(curVal + 1), StandardCharsets.UTF_8))
                                .get();

                        return "broker-" + curVal;
                    }
                }
            } finally {
                lock.unlock(BROKER_ID_LOCK_KEY);
            }
        }
    }
}