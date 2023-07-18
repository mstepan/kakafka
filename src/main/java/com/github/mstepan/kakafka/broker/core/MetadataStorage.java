package com.github.mstepan.kakafka.broker.core;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class MetadataStorage {

    private final AtomicReference<String> leaderBrokerNameRef = new AtomicReference<>();

    private final AtomicReference<List<LiveBroker>> liveBrokersRef = new AtomicReference<>();

    public void setLeader(String brokerName) {
        Objects.requireNonNull(brokerName, "null 'brokerName' during set operation");
        this.leaderBrokerNameRef.set(brokerName);
    }

    public void setLiveBrokers(List<LiveBroker> liveBrokers) {
        liveBrokersRef.set(Collections.unmodifiableList(liveBrokers));
    }

    public MetadataState getMetadataState() {
        return new MetadataState(leaderBrokerNameRef.get(), liveBrokersRef.get());
    }

    //    public CompletableFuture<GetResponse> getMetadataState() {
    //        //
    //        // todo: we should query metadata state in background thread
    //        // todo: here we just need to obtain in-memory data and send back to client
    //        //
    //        return CompletableFuture.supplyAsync(
    //                () -> {
    //                    try (Client client =
    // Client.builder().endpoints(config.etcdEndpoint()).build();
    //                            KV kvClient = client.getKVClient()) {
    //                        try {
    //                            return kvClient.get(
    //
    // EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX),
    //                                            GetOption.newBuilder().isPrefix(true).build())
    //                                    .get();
    //                        } catch (InterruptedException | ExecutionException ex) {
    //                            ex.printStackTrace();
    //                            return null;
    //                        }
    //                    }
    //                });
    //    }
}
