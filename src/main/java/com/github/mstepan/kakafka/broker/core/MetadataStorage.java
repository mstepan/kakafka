package com.github.mstepan.kakafka.broker.core;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MetadataStorage {

    private final BrokerConfig config;

    public MetadataStorage(BrokerConfig config) {
        this.config = config;
    }

    private volatile String leaderBrokerName;

    public void setLeader(String brokerName) {
        this.leaderBrokerName = brokerName;
    }

    public MetadataState getMetadataState() {

        List<LiveBroker> liveBrokers = new ArrayList<>();

        // todo: we should query metadata state in background thread
        // todo: here we just need to obtain in-memory data and send back to client, b/c blockign
        // calls inside
        // todo: netty event loop is considered as bad practice
        try (Client client = Client.builder().endpoints(config.etcdEndpoint()).build();
                KV kvClient = client.getKVClient()) {

            GetResponse getResp =
                    kvClient.get(
                                    EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX),
                                    GetOption.newBuilder().isPrefix(true).build())
                            .get();

            for (KeyValue keyValue : getResp.getKvs()) {

                String brokerIdPath = keyValue.getKey().toString(StandardCharsets.US_ASCII);

                final String brokerName = brokerIdPath.substring(brokerIdPath.lastIndexOf("/") + 1);
                final String brokerUrl = keyValue.getValue().toString(StandardCharsets.US_ASCII);

                liveBrokers.add(new LiveBroker(brokerName, brokerUrl));
            }

        } catch (InterruptedException interEx) {
            interEx.printStackTrace();
            Thread.currentThread().interrupt();
        } catch (ExecutionException execEx) {
            execEx.printStackTrace();
        }

        return new MetadataState(leaderBrokerName, liveBrokers);
    }
}
