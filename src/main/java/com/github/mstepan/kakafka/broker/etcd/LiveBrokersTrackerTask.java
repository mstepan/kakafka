package com.github.mstepan.kakafka.broker.etcd;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.utils.BrokerMdcPropagator;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This task tracks live brokers in a separate thread and updates 'MetadataStorage.liveBrokers' map
 * properly.
 */
public class LiveBrokersTrackerTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final ByteSequence BROKER_KEY_PREFIX =
            EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX);

    private final BrokerContext brokerCtx;

    public LiveBrokersTrackerTask(BrokerContext brokerCtx) {
        this.brokerCtx = brokerCtx;
    }

    private final BlockingQueue<WatchEvent> brokerNewEvents = new ArrayBlockingQueue<>(1024);

    @Override
    public void run() {
        try (BrokerMdcPropagator notUsed =
                new BrokerMdcPropagator(brokerCtx.config().brokerName())) {
            Thread.currentThread().setName("LiveBrokersTrackerTask");
            LOG.info("Live brokers tracker DEDICATED thread started");

            watchForChanges();

            fetchLiveBrokersFromEtcd();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    WatchEvent newEvent = brokerNewEvents.take();

                    if (newEvent.getEventType() == WatchEvent.EventType.PUT) {

                        final String brokerId = extractBrokerId(newEvent.getKeyValue());
                        final String brokerUrl = extractBrokerUrl(newEvent.getKeyValue());

                        brokerCtx.metadata().addLiveBroker(new LiveBroker(brokerId, brokerUrl));
                    } else if (newEvent.getEventType() == WatchEvent.EventType.DELETE) {
                        brokerCtx
                                .metadata()
                                .deleteLiveBroker(extractBrokerId(newEvent.getKeyValue()));
                    }

                } catch (InterruptedException interEx) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void fetchLiveBrokersFromEtcd() {
        try {
            @SuppressWarnings("resource")
            final KV kvClient = brokerCtx.etcdClientHolder().kvClient();

            GetResponse getResp =
                    kvClient.get(BROKER_KEY_PREFIX, GetOption.newBuilder().isPrefix(true).build())
                            .get();

            LOG.info("Metadata state obtained from 'etcd'");

            List<LiveBroker> liveBrokers = new ArrayList<>();
            for (KeyValue keyValue : getResp.getKvs()) {

                // 'keyValue.key' = '/kakafka/brokers/broker-3b93e71d-df46-4a4a-98ac-41a1eaf9216c'
                final String brokerId = extractBrokerId(keyValue);

                // 'keyValue.value' = 'localhost:9090'
                final String brokerUrl = extractBrokerUrl(keyValue);

                liveBrokers.add(new LiveBroker(brokerId, brokerUrl));
            }

            brokerCtx.metadata().addLiveBrokers(liveBrokers);

        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
        }
    }

    private static String extractBrokerId(KeyValue keyValue) {
        String brokerIdPath = keyValue.getKey().toString(StandardCharsets.US_ASCII);
        return brokerIdPath.substring(brokerIdPath.lastIndexOf("/") + 1);
    }

    private static String extractBrokerUrl(KeyValue keyValue) {
        return keyValue.getValue().toString(StandardCharsets.US_ASCII);
    }

    private void watchForChanges() {

        @SuppressWarnings("resource")
        Watch watchClient = brokerCtx.etcdClientHolder().watchClient();

        //
        // Start watching for ACTIVE brokers.
        // Event will be received inside 'vert.x-eventloop-thread-0' thread.
        //
        watchClient.watch(
                BROKER_KEY_PREFIX,
                WatchOption.newBuilder().isPrefix(true).withRevision(0L).build(),
                new Watch.Listener() {
                    @Override
                    public void onNext(WatchResponse watchResponse) {

                        try (BrokerMdcPropagator notUsed =
                                new BrokerMdcPropagator(brokerCtx.config().brokerName())) {

                            List<WatchEvent> events = watchResponse.getEvents();

                            for (WatchEvent singleEvent : events) {
                                if (singleEvent.getEventType()
                                        == WatchEvent.EventType.UNRECOGNIZED) {
                                    LOG.warn("UNRECOGNIZED event");
                                } else {
                                    // PUT & DELETE only here
                                    if (!brokerNewEvents.offer(singleEvent)) {
                                        LOG.error(
                                                "Can't insert events into 'brokerNewEvents', queue is FULL.");
                                    }
                                }
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable ex) {
                        try (BrokerMdcPropagator notUsed =
                                new BrokerMdcPropagator(brokerCtx.config().brokerName())) {
                            LOG.error(
                                    "Error during watching events on etcd value '%s'"
                                            .formatted(BROKER_KEY_PREFIX),
                                    ex);
                        }
                    }

                    @Override
                    public void onCompleted() {}
                });
    }
}
