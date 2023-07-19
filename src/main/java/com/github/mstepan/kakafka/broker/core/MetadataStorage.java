package com.github.mstepan.kakafka.broker.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class MetadataStorage {

    private final AtomicReference<String> leaderBrokerNameRef = new AtomicReference<>();

    private final Map<String, LiveBroker> liveBrokers = new ConcurrentHashMap<>();

    private final Queue<LiveBroker> brokersForSampling = new ConcurrentLinkedQueue<>();

    public void setLeader(String brokerName) {
        Objects.requireNonNull(brokerName, "null 'brokerName' during set operation");
        this.leaderBrokerNameRef.set(brokerName);
    }

    public void addLiveBrokers(List<LiveBroker> brokers) {
        for (LiveBroker curBroker : brokers) {
            liveBrokers.put(curBroker.id(), curBroker);
            brokersForSampling.add(curBroker);
        }
    }

    public MetadataState getMetadataState() {
        return new MetadataState(leaderBrokerNameRef.get(), liveBrokers.values());
    }

    public void addLiveBroker(LiveBroker newLiveBroker) {
        liveBrokers.put(newLiveBroker.id(), newLiveBroker);
        brokersForSampling.add(newLiveBroker);
    }

    public void deleteLiveBroker(String brokerId) {
        liveBrokers.remove(brokerId);
    }

    public Collection<LiveBroker> getLiveBrokers() {
        return liveBrokers.values();
    }

    /** Select live brokers in round-robin fashion here. */
    public List<LiveBroker> getSamplingOfLiveBrokers(int count) {

        List<LiveBroker> sampling = new ArrayList<>();

        for (int i = 0; i < count; ++i) {

            while (true) {
                LiveBroker curBroker = brokersForSampling.poll();

                // check if broker is still ALIVE
                if (liveBrokers.containsKey(curBroker.id())) {
                    sampling.add(curBroker);
                    brokersForSampling.add(curBroker);
                    break;
                }
            }
        }

        return sampling;
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
