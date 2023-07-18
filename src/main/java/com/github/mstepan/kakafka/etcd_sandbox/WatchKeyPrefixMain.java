package com.github.mstepan.kakafka.etcd_sandbox;

import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WatchKeyPrefixMain {

    private static final String ETCD_ENDPOINT = "http://localhost:2379";

    private static final String BROKER_KEY_PREFIX = "/kakafka/brokers";

    public static void main(String[] args) throws Exception {
        try (Client client = Client.builder().endpoints(ETCD_ENDPOINT).build();
                Watch watchClient = client.getWatchClient()) {

            watchClient.watch(
                    EtcdUtils.toByteSeq(BROKER_KEY_PREFIX),
                    WatchOption.newBuilder().isPrefix(true).withRevision(0L).build(),
                    new Watch.Listener() {
                        @Override
                        public void onNext(WatchResponse watchResponse) {

                            System.out.printf("Thread: %s%n", Thread.currentThread().getName());

                            List<WatchEvent> events = watchResponse.getEvents();

                            for (WatchEvent singleEvent : events) {
                                if (singleEvent.getEventType() == WatchEvent.EventType.PUT) {
                                    System.out.printf(
                                            "PUT %s => %s%n",
                                            singleEvent.getKeyValue().getKey().toString(),
                                            singleEvent.getKeyValue().getValue().toString());

                                } else if (singleEvent.getEventType()
                                        == WatchEvent.EventType.DELETE) {
                                    System.out.printf(
                                            "DELETE %s%n",
                                            singleEvent.getKeyValue().getKey().toString());

                                } else {
                                    System.out.println("UNRECOGNIZED event");
                                }
                            }
                        }

                        @Override
                        public void onError(Throwable ex) {
                            ex.printStackTrace();
                        }

                        @Override
                        public void onCompleted() {}
                    });

            System.out.printf("Watching prefix %s%n", BROKER_KEY_PREFIX);

            TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
        }
    }
}
