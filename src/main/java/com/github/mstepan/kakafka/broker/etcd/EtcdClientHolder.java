package com.github.mstepan.kakafka.broker.etcd;

import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Watch;

public record EtcdClientHolder(
        Lease leaseClient, Election electionClient, KV kvClient, Watch watchClient) {}
