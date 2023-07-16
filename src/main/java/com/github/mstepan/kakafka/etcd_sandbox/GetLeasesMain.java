package com.github.mstepan.kakafka.etcd_sandbox;

import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.options.PutOption;
import java.util.concurrent.TimeUnit;

public class GetLeasesMain {

    private static final String ETCD_ENDPOINT = "http://localhost:2379";

    private static final String BROKER_KEY_PREFIX = "/kakafka/brokers/%s";

    private static final long LEASE_TTL_IN_SEC = 3L;

    private static final long OPERATION_TIMEOUT_IN_SEC = 3L;
    private static final TimeUnit OPERATION_TIMEOUT_UNIT = TimeUnit.SECONDS;

    /** Should be less than 'LEASE_TTL_IN_SEC' value */
    private static final long THREAD_SLEEP_TIME_IN_SEC = 1L;

    public static void main(String[] args) throws Exception {

        System.out.println("Create 'etcd' lease and query all active leases.");

        try (Client client = Client.builder().endpoints(ETCD_ENDPOINT).build();
                KV kvClient = client.getKVClient();
                Lease leaseClient = client.getLeaseClient()) {

            LeaseGrantResponse leaseResp =
                    leaseClient
                            .grant(
                                    LEASE_TTL_IN_SEC,
                                    OPERATION_TIMEOUT_IN_SEC,
                                    OPERATION_TIMEOUT_UNIT)
                            .get();

            while (true) {

                String brokerId = args[0];
                String brokerUrl = args[1];

                kvClient.put(
                        EtcdUtils.toByteSeq(BROKER_KEY_PREFIX.formatted(brokerId)),
                        EtcdUtils.toByteSeq(brokerUrl),
                        PutOption.newBuilder().withLeaseId(leaseResp.getID()).build());

                TimeUnit.SECONDS.sleep(THREAD_SLEEP_TIME_IN_SEC);
                // https://github.com/etcd-io/jetcd/blob/main/jetcd-core/src/main/java/io/etcd/jetcd/Lease.java
                leaseClient.keepAliveOnce(leaseResp.getID()).get();
            }
        }
    }
}
