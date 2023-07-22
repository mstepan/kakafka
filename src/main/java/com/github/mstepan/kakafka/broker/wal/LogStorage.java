package com.github.mstepan.kakafka.broker.wal;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.io.IOUtils;
import java.nio.file.Path;

public record LogStorage(BrokerConfig config) {

    public void init() {
        Path brokerDataFolder = Path.of(config.dataFolder(), config().brokerName());
        IOUtils.createFolderIfNotExist(config.brokerName(), brokerDataFolder);
    }

    public void appendMessage(String topicName, int partitionIdx, StringTopicMessage msg) {}
}
