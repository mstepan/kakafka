package com.github.mstepan.kakafka.broker.etcd;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
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
public class KeepAliveAndLeaderElectionTask implements Runnable {

    private static final ByteSequence LEADER_KEY = EtcdUtils.toByteSeq("/kakafka/leader");

    private static final String BROKER_KEY_PREFIX_TEMPLATE = BrokerConfig.BROKER_KEY_PREFIX + "/%s";

    /** Should be less or equal to 'LEASE_TTL_IN_SEC' value */
    private static final long LEASE_GRANT_OPERATION_TIMEOUT_IN_SEC = 3L;

    private static final long LEASE_TTL_IN_SEC = 3L;

    /** Should be less than 'LEASE_TTL_IN_SEC' value */
    private static final long THREAD_SLEEP_TIME_IN_SEC = 1L;

    private final BrokerConfig config;

    private final MetadataStorage metadata;

    public KeepAliveAndLeaderElectionTask(BrokerConfig config, MetadataStorage metadata) {
        this.config = config;
        this.metadata = metadata;
    }

    @Override
    public void run() {
        try (Client client = Client.builder().endpoints(config.etcdEndpoint()).build();
                Lease lease = client.getLeaseClient();
                KV kvClient = client.getKVClient();
                Election electionClient = client.getElectionClient()) {

            LeaseGrantResponse leaseResp =
                    lease.grant(
                                    LEASE_TTL_IN_SEC,
                                    LEASE_GRANT_OPERATION_TIMEOUT_IN_SEC,
                                    TimeUnit.SECONDS)
                            .get();

            System.out.printf("[%s] etcd LEASE granted%n", config.brokerName());

            kvClient.put(
                    EtcdUtils.toByteSeq(BROKER_KEY_PREFIX_TEMPLATE.formatted(config.brokerName())),
                    EtcdUtils.toByteSeq(config.url()),
                    PutOption.newBuilder().withLeaseId(leaseResp.getID()).build());

            System.out.printf(
                    "[%s] registering active broker at prefix '%s' %n",
                    config.brokerName(), BROKER_KEY_PREFIX_TEMPLATE.formatted(config.brokerName()));

            electionClient.observe(LEADER_KEY, new LeaderElectionListener(config, metadata));

            // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Election.java
            electionClient.campaign(
                    LEADER_KEY, leaseResp.getID(), EtcdUtils.toByteSeq(config.brokerName()));

            System.out.printf("[%s] etcd LEADER ELECTION started%n", config.brokerName());

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

    private record LeaderElectionListener(BrokerConfig config, MetadataStorage metadata)
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
                    config.brokerName(), leaderKeyAndValue.getKey(), leaderKeyAndValue.getValue());

            metadata.setLeader(leaderKeyAndValue.getValue().toString(StandardCharsets.US_ASCII));
        }

        @Override
        public void onError(Throwable ex) {
            ex.printStackTrace();
        }

        @Override
        public void onCompleted() {
            System.out.printf("[%s] Election done%n", config.brokerName());
        }
    }
}
