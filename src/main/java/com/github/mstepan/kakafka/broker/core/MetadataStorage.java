package com.github.mstepan.kakafka.broker.core;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class MetadataStorage {

    private final BrokerConfig config;

    public MetadataStorage(BrokerConfig config) {
        this.config = config;
    }

    private final AtomicReference<String> leaderBrokerNameRef = new AtomicReference<>();

    public void setLeader(String brokerName) {
        Objects.requireNonNull(brokerName, "null 'brokerName' during set operation");
        this.leaderBrokerNameRef.set(brokerName);
    }

    public String getLeaderBrokerName() {
        String leaderBrokerName = leaderBrokerNameRef.get();
        Objects.requireNonNull(leaderBrokerName, "null 'leaderBrokerName' during get operation");
        return leaderBrokerName;
    }

    public CompletableFuture<GetResponse> getMetadataState() {
        //
        // todo: we should query metadata state in background thread
        // todo: here we just need to obtain in-memory data and send back to client
        //
        return CompletableFuture.supplyAsync(
                () -> {
                    try (Client client = Client.builder().endpoints(config.etcdEndpoint()).build();
                            KV kvClient = client.getKVClient()) {
                        try {
                            return kvClient.get(
                                            EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX),
                                            GetOption.newBuilder().isPrefix(true).build())
                                    .get();
                        } catch (InterruptedException | ExecutionException ex) {
                            ex.printStackTrace();
                            return null;
                        }
                    }
                });
    }
}
