package com.github.mstepan.kakafka.broker.wal;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

final class LogStorageTest {

    private static final String BROKER_DATA_FOLDER = "data";

    private static final String BROKER_NAME = "test-broker-1";

    private BrokerConfig brokerConfig;
    private LogStorage logStorage;

    @BeforeEach
    void setup() {
        IOUtils.delete(BROKER_DATA_FOLDER);
        brokerConfig = new BrokerConfig(BROKER_NAME, -1, "<undefined>", BROKER_DATA_FOLDER);
        logStorage = new LogStorage(brokerConfig);
    }

    @AfterEach
    void tearDown() {
        logStorage = null;
        brokerConfig = null;
    }

    @Test
    void appendMessage() {
        logStorage.appendMessage("topic-1", 0, new StringTopicMessage("key-111", "value-111"));
        logStorage.appendMessage("topic-1", 0, new StringTopicMessage("key-111", "value-222"));
        logStorage.appendMessage("topic-1", 0, new StringTopicMessage("key-111", "value-333"));
    }

    @Test
    void getMessage() {
        final String topicName = "topic-1";
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-111", "value-111"));

        StringTopicMessage msg = logStorage.getMessage(topicName, 0, 0);

        assertNotNull(msg, "Can't retrieve message");
    }
}
