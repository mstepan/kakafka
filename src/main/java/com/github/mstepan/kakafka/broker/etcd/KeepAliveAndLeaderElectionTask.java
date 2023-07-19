package com.github.mstepan.kakafka.broker.etcd;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * LEADER ELECTION. This class participates in leader election. If current broker is selected as a
 * leader, the following key-value created '/kakafka/leader/<lease-ID>' => '<broker-name>'. Example:
 * '/kakafka/leader/694d895902aac425' => 'broker-dbea19ad-aa1e-4b6c-8673-7c3fbc9a7755'
 *
 * <p>ACTIVE BROKER LIST. All active brokers create the following key-value with appropriate LEASE:
 * '/kakafka/brokers/<broker-name>' => '<broker full URL>'. Example:
 * '/kakafka/brokers/broker-dbea19ad-aa1e-4b6c-8673-7c3fbc9a7755' => "localhost:9090"
 */
public final class KeepAliveAndLeaderElectionTask implements Runnable {

    private static final ByteSequence LEADER_KEY = EtcdUtils.toByteSeq("/kakafka/leader");

    private static final String BROKER_KEY_PREFIX_TEMPLATE = BrokerConfig.BROKER_KEY_PREFIX + "/%s";

    /** Should be less or equal to 'LEASE_TTL_IN_SEC' value */
    private static final long LEASE_GRANT_OPERATION_TIMEOUT_IN_SEC = 3L;

    private static final long LEASE_TTL_IN_SEC = 3L;

    /** Should be less than 'LEASE_TTL_IN_SEC' value */
    private static final long THREAD_SLEEP_TIME_IN_SEC = 1L;

    private final BrokerContext brokerCtx;

    public KeepAliveAndLeaderElectionTask(BrokerContext brokerCtx) {
        this.brokerCtx = brokerCtx;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("KeepAliveAndLeaderElectionTask");
        try {
            final Lease leaseClient = brokerCtx.etcdClientHolder().leaseClient();
            final KV kvClient = brokerCtx.etcdClientHolder().kvClient();
            final Election electionClient = brokerCtx.etcdClientHolder().electionClient();

            final String brokerName = brokerCtx.config().brokerName();

            final BrokerConfig config = brokerCtx.config();
            final MetadataStorage metadata = brokerCtx.metadata();

            LeaseGrantResponse leaseResp =
                    leaseClient
                            .grant(
                                    LEASE_TTL_IN_SEC,
                                    LEASE_GRANT_OPERATION_TIMEOUT_IN_SEC,
                                    TimeUnit.SECONDS)
                            .get();

            System.out.printf("[%s] etcd LEASE granted%n", brokerName);

            kvClient.put(
                    EtcdUtils.toByteSeq(BROKER_KEY_PREFIX_TEMPLATE.formatted(brokerName)),
                    EtcdUtils.toByteSeq(config.url()),
                    PutOption.newBuilder().withLeaseId(leaseResp.getID()).build());

            System.out.printf(
                    "[%s] registering active broker at prefix '%s' %n",
                    config.brokerName(), BROKER_KEY_PREFIX_TEMPLATE.formatted(brokerName));

            electionClient.observe(LEADER_KEY, new LeaderElectionListener(brokerName, metadata));

            // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Election.java
            electionClient.campaign(LEADER_KEY, leaseResp.getID(), EtcdUtils.toByteSeq(brokerName));

            System.out.printf("[%s] etcd LEADER ELECTION started%n", brokerName);

            while (!Thread.currentThread().isInterrupted()) {
                TimeUnit.SECONDS.sleep(THREAD_SLEEP_TIME_IN_SEC);
                // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Lease.java
                leaseClient.keepAliveOnce(leaseResp.getID()).get();
                //                System.out.printf("[%s] lease keep alive%n", config.brokerName());
            }
        } catch (InterruptedException interEx) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException execEx) {
            execEx.printStackTrace();
        }
    }

    private record LeaderElectionListener(String brokerName, MetadataStorage metadata)
            implements Election.Listener {

        @Override
        public void onNext(LeaderResponse leaderResponse) {

            KeyValue leaderKeyAndValue = leaderResponse.getKv();

            /*
            key = /kakafka/leader/694d895902aac425
            value = broker-dbea19ad-aa1e-4b6c-8673-7c3fbc9a7755
            */
            System.out.printf(
                    "[%s] Leader selected, key = '%s', value = '%s' %n",
                    brokerName, leaderKeyAndValue.getKey(), leaderKeyAndValue.getValue());

            final String leaderName =
                    leaderKeyAndValue.getValue().toString(StandardCharsets.US_ASCII);
            metadata.setLeader(leaderName);
        }

        @Override
        public void onError(Throwable ex) {
            ex.printStackTrace();
        }

        @Override
        public void onCompleted() {
            System.out.printf("[%s] Election done%n", brokerName);
        }
    }
}
