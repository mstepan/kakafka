package com.github.mstepan.kakafka.broker.etcd;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import com.github.mstepan.kakafka.broker.utils.ThreadUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MetadataRetrieverTask implements Runnable {

    private static final long ETC_METADATA_POLL_INTERVAL_IN_SEC = 5L;

    private final BrokerConfig config;

    private final MetadataStorage metaStorage;

    public MetadataRetrieverTask(BrokerConfig config, MetadataStorage metaStorage) {
        this.config = config;
        this.metaStorage = metaStorage;
    }

    @Override
    public void run() {

        System.out.printf("[%s] Metadata retriever thread started", config.brokerName());

        while (!Thread.currentThread().isInterrupted()) {

            try (Client client = Client.builder().endpoints(config.etcdEndpoint()).build();
                    KV kvClient = client.getKVClient()) {
                try {
                    GetResponse getResp =
                            kvClient.get(
                                            EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX),
                                            GetOption.newBuilder().isPrefix(true).build())
                                    .get();

                    //                    System.out.printf(
                    //                            "[%s] metadata state obtained from 'etcd' %n",
                    // config.brokerName());

                    List<LiveBroker> liveBrokers = new ArrayList<>();
                    for (KeyValue keyValue : getResp.getKvs()) {

                        String brokerIdPath = keyValue.getKey().toString(StandardCharsets.US_ASCII);

                        final String brokerName =
                                brokerIdPath.substring(brokerIdPath.lastIndexOf("/") + 1);
                        final String brokerUrl =
                                keyValue.getValue().toString(StandardCharsets.US_ASCII);

                        liveBrokers.add(new LiveBroker(brokerName, brokerUrl));
                    }

                    metaStorage.setLiveBrokers(liveBrokers);

                } catch (InterruptedException | ExecutionException ex) {
                    ex.printStackTrace();
                }
            }

            ThreadUtils.sleepSec(ETC_METADATA_POLL_INTERVAL_IN_SEC);
        }
    }
}
