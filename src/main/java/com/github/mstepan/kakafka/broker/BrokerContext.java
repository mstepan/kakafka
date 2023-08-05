package com.github.mstepan.kakafka.broker;

import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.core.storage.LogStorage;
import com.github.mstepan.kakafka.broker.etcd.EtcdClientHolder;

public record BrokerContext(
        BrokerConfig config,
        MetadataStorage metadata,
        EtcdClientHolder etcdClientHolder,
        LogStorage logStorage) {}
