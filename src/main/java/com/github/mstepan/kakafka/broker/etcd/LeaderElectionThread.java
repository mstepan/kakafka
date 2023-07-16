package com.github.mstepan.kakafka.broker.etcd;

import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LeaderElectionThread implements Runnable {

    private static final String ETCD_ENDPOINT = "http://localhost:2379";

    private static final ByteSequence LEADER_KEY =
            ByteSequence.from("/kakafka/leader", StandardCharsets.UTF_8);

    private static final long LEASE_TTL_IN_SEC = 3L;

    /** Should be less than 'LEASE_TTL_IN_SEC' value */
    private static final long THREAD_SLEEP_TIME_IN_SEC = 1L;

    private final String brokerName;

    private final MetadataStorage metadata;

    public LeaderElectionThread(String brokerName, MetadataStorage metadata) {
        this.brokerName = brokerName;
        this.metadata = metadata;
    }

    @Override
    public void run() {
        try (Client client = Client.builder().endpoints(ETCD_ENDPOINT).build();
                Lease lease = client.getLeaseClient();
                Election electionClient = client.getElectionClient()) {

            LeaseGrantResponse leaseResp =
                    lease.grant(LEASE_TTL_IN_SEC, 3L, TimeUnit.SECONDS).get();

            System.out.printf("[%s] etcd LEASE granted%n", brokerName);

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
                            System.out.printf(
                                    "[%s] Leader selected, key = '%s', value = '%s' %n",
                                    brokerName,
                                    leaderKeyAndValue.getKey(),
                                    leaderKeyAndValue.getValue());

                            metadata.setLeader(
                                    leaderKeyAndValue
                                            .getValue()
                                            .toString(StandardCharsets.US_ASCII));
                        }

                        @Override
                        public void onError(Throwable ex) {
                            ex.printStackTrace();
                        }

                        @Override
                        public void onCompleted() {
                            System.out.printf("[%s] Election done%n", brokerName);
                        }
                    });

            // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Election.java
            electionClient.campaign(LEADER_KEY, leaseResp.getID(), serverId);

            System.out.printf("[%s] etcd LEADER ELECTION started%n", brokerName);

            while (!Thread.currentThread().isInterrupted()) {
                TimeUnit.SECONDS.sleep(THREAD_SLEEP_TIME_IN_SEC);
                // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Lease.java
                lease.keepAliveOnce(leaseResp.getID()).get();
            }
        } catch (InterruptedException interEx) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException execEx) {
            execEx.printStackTrace();
        }
    }
}
