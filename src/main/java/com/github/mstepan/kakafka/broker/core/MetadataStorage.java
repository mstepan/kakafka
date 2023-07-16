package com.github.mstepan.kakafka.broker.core;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.utils.EtcdUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import java.nio.charset.StandardCharsets;
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

    public String getMetadataSnapshot() {

        StringBuilder brokersData = new StringBuilder();

        try (Client client = Client.builder().endpoints(config.etcdEndpoint()).build();
                KV kvClient = client.getKVClient()) {

            GetResponse getResp =
                    kvClient.get(
                                    EtcdUtils.toByteSeq(BrokerConfig.BROKER_KEY_PREFIX),
                                    GetOption.newBuilder().isPrefix(true).build())
                            .get();

            for (KeyValue keyValue : getResp.getKvs()) {

                String brokerIdPath = keyValue.getKey().toString(StandardCharsets.US_ASCII);

                brokersData.append(
                        """
                        {
                            "id": "%s",
                            "url": "%s"
                        }
                        """
                                .formatted(
                                        brokerIdPath.substring(brokerIdPath.lastIndexOf("/") + 1),
                                        keyValue.getValue().toString(StandardCharsets.US_ASCII)));
            }

        } catch (InterruptedException interEx) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException execEx) {
            execEx.printStackTrace();
        }

        return """
            {
                "leader": "%s"
                "brokers": ["%s"]
            }
            """
                .formatted(leaderBrokerName, brokersData);
    }
}
