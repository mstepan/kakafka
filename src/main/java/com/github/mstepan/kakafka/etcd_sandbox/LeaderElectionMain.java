package com.github.mstepan.kakafka.etcd_sandbox;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElectionMain {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String ETCD_ENDPOINT = "http://localhost:2379";

    private static final ByteSequence LEADER_KEY =
            ByteSequence.from("/kakafka/leader", StandardCharsets.UTF_8);

    private static final long LEASE_TTL_IN_SEC = 3L;

    /** Should be less than 'LEASE_TTL_IN_SEC' value */
    private static final long THREAD_SLEEP_TIME_IN_SEC = 1L;

    public static void main(String[] args) throws Exception {

        try (Client client = Client.builder().endpoints(ETCD_ENDPOINT).build();
                Lease leaseClient = client.getLeaseClient();
                Election electionClient = client.getElectionClient()) {

            LeaseGrantResponse leaseResp =
                    leaseClient.grant(LEASE_TTL_IN_SEC, 3L, TimeUnit.SECONDS).get();

            final String brokerName =
                    "broker-%s"
                            .formatted(
                                    UUID.randomUUID()); // generateUniqueBrokerName(client, lease);

            LOG.info("'{}' started", brokerName);

            ByteSequence serverId = ByteSequence.from(brokerName, StandardCharsets.UTF_8);

            electionClient.observe(
                    LEADER_KEY,
                    new Election.Listener() {
                        @Override
                        public void onNext(LeaderResponse leaderResponse) {

                            KeyValue leaderKeyAndValue = leaderResponse.getKv();

                            /*
                            key = /kakafka/leader/694d895902aac425
                            value = broker-dbea19ad-aa1e-4b6c-8673-7c3fbc9a7755
                            */
                            LOG.info(
                                    "Leader selected, key = {}, value = {}",
                                    leaderKeyAndValue.getKey(),
                                    leaderKeyAndValue.getValue());
                        }

                        @Override
                        public void onError(Throwable ex) {
                            ex.printStackTrace();
                        }

                        @Override
                        public void onCompleted() {
                            LOG.info("Election done");
                        }
                    });

            // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Election.java
            electionClient.campaign(LEADER_KEY, leaseResp.getID(), serverId);

            while (true) {
                TimeUnit.SECONDS.sleep(THREAD_SLEEP_TIME_IN_SEC);
                // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Lease.java
                leaseClient.keepAliveOnce(leaseResp.getID()).get();
            }
        }
    }
}
