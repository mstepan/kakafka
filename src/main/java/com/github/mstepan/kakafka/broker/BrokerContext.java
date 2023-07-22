package com.github.mstepan.kakafka.broker;

import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.etcd.EtcdClientHolder;
import com.github.mstepan.kakafka.broker.wal.LogStorage;

public record BrokerContext(
        BrokerConfig config,
        MetadataStorage metadata,
        EtcdClientHolder etcdClientHolder,
        LogStorage logStorage) {}
