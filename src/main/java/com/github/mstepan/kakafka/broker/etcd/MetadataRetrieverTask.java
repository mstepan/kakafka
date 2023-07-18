package com.github.mstepan.kakafka.broker.etcd;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.BrokerContext;
import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import com.github.mstepan.kakafka.broker.utils.ThreadUtils;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MetadataRetrieverTask implements Runnable {

    private static final ByteSequence BROKER_KEY_PREFIX =
            EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX);

    private static final long ETC_METADATA_POLL_INTERVAL_IN_SEC = 3L;

    private final BrokerContext brokerCtx;

    public MetadataRetrieverTask(BrokerContext brokerCtx) {
        this.brokerCtx = brokerCtx;
    }

    @Override
    public void run() {

        System.out.printf(
                "[%s] Metadata retriever thread started", brokerCtx.config().brokerName());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                final KV kvClient = brokerCtx.etcdClientHolder().kvClient();

                GetResponse getResp =
                        kvClient.get(
                                        BROKER_KEY_PREFIX,
                                        GetOption.newBuilder().isPrefix(true).build())
                                .get();

                //                System.out.printf(
                //                        "[%s] metadata state obtained from 'etcd' %n",
                // config.brokerName());

                List<LiveBroker> liveBrokers = new ArrayList<>();
                for (KeyValue keyValue : getResp.getKvs()) {

                    // 'brokerIdPath' example
                    // '/kakafka/brokers/broker-3b93e71d-df46-4a4a-98ac-41a1eaf9216c'
                    String brokerIdPath = keyValue.getKey().toString(StandardCharsets.US_ASCII);

                    final String brokerName =
                            brokerIdPath.substring(brokerIdPath.lastIndexOf("/") + 1);
                    final String brokerUrl =
                            keyValue.getValue().toString(StandardCharsets.US_ASCII);

                    liveBrokers.add(new LiveBroker(brokerName, brokerUrl));
                }

                brokerCtx.metadata().setLiveBrokers(liveBrokers);

            } catch (InterruptedException | ExecutionException ex) {
                ex.printStackTrace();
            }

            ThreadUtils.sleepSec(ETC_METADATA_POLL_INTERVAL_IN_SEC);
        }
    }
}
