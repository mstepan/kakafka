package com.github.mstepan.kakafka.broker.etcd;

import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;

public record EtcdClientHolder(Lease lease, Election electionClient, KV kvClient) {}
