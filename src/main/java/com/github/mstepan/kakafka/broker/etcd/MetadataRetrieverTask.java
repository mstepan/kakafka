package com.github.mstepan.kakafka.broker.etcd;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import com.github.mstepan.kakafka.broker.utils.ThreadUtils;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class MetadataRetrieverTask implements Runnable {

    private static final long SLEEP_TIMEOUT_IN_SEC = 2L;

    private static final ByteSequence BROKER_KEY_PREFIX =
            EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX);

    private final BrokerContext brokerCtx;

    public MetadataRetrieverTask(BrokerContext brokerCtx) {
        this.brokerCtx = brokerCtx;
    }

    private final AtomicInteger liveBrokerEventsCount = new AtomicInteger(0);

    @Override
    public void run() {
        System.out.printf(
                "[%s] Metadata retriever thread started", brokerCtx.config().brokerName());

        watchForChanges();

        fetchLiveBrokersFromEtcd(liveBrokerEventsCount.get());

        while (!Thread.currentThread().isInterrupted()) {

            ThreadUtils.sleepSec(SLEEP_TIMEOUT_IN_SEC);

            int newEventsCount = liveBrokerEventsCount.get();

            if (newEventsCount != 0) {
                fetchLiveBrokersFromEtcd(newEventsCount);
            }
        }
    }

    private void fetchLiveBrokersFromEtcd(int eventsCount) {
        try {
            final KV kvClient = brokerCtx.etcdClientHolder().kvClient();

            GetResponse getResp =
                    kvClient.get(BROKER_KEY_PREFIX, GetOption.newBuilder().isPrefix(true).build())
                            .get();

            ThreadUtils.decrementBy(liveBrokerEventsCount, eventsCount);

            System.out.printf(
                    "[%s] metadata state obtained from 'etcd' %n", brokerCtx.config().brokerName());

            List<LiveBroker> liveBrokers = new ArrayList<>();
            for (KeyValue keyValue : getResp.getKvs()) {

                // 'brokerIdPath' example
                // '/kakafka/brokers/broker-3b93e71d-df46-4a4a-98ac-41a1eaf9216c'
                String brokerIdPath = keyValue.getKey().toString(StandardCharsets.US_ASCII);

                final String brokerName = brokerIdPath.substring(brokerIdPath.lastIndexOf("/") + 1);
                final String brokerUrl = keyValue.getValue().toString(StandardCharsets.US_ASCII);

                liveBrokers.add(new LiveBroker(brokerName, brokerUrl));
            }

            brokerCtx.metadata().setLiveBrokers(liveBrokers);

        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
        }
    }

    private void watchForChanges() {

        Watch watchClient = brokerCtx.etcdClientHolder().watchClient();

        watchClient.watch(
                BROKER_KEY_PREFIX,
                WatchOption.newBuilder().isPrefix(true).withRevision(0L).build(),
                new Watch.Listener() {
                    @Override
                    public void onNext(WatchResponse watchResponse) {
                        List<WatchEvent> events = watchResponse.getEvents();

                        for (WatchEvent singleEvent : events) {
                            if (singleEvent.getEventType() == WatchEvent.EventType.PUT) {
                                System.out.printf(
                                        "PUT %s => %s%n",
                                        singleEvent.getKeyValue().getKey().toString(),
                                        singleEvent.getKeyValue().getValue().toString());

                            } else if (singleEvent.getEventType() == WatchEvent.EventType.DELETE) {
                                System.out.printf(
                                        "DELETE %s%n",
                                        singleEvent.getKeyValue().getKey().toString());

                            } else {
                                System.out.println("UNRECOGNIZED event");
                            }

                            liveBrokerEventsCount.incrementAndGet();

                            //                            ThreadUtils.notifyAllOn(mutex);
                        }
                    }

                    @Override
                    public void onError(Throwable ex) {
                        ex.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {}
                });
    }
}
