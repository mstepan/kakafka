package com.github.mstepan.kakafka.broker.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    void appendMessageMultipleMessages() {
        logStorage.appendMessage("topic-1", 0, new StringTopicMessage("key-111", "value-111"));
        logStorage.appendMessage("topic-1", 0, new StringTopicMessage("key-111", "value-222"));
        logStorage.appendMessage("topic-1", 0, new StringTopicMessage("key-111", "value-333"));
    }

    @Test
    void getSingleMessageNormalCase() {
        final String topicName = "topic-1";
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-111", "value-111"));

        StringTopicMessage msg = logStorage.getMessage(topicName, 0, 0);

        assertMessage("key-111", "value-111", msg);
    }

    @Test
    void getMultipleMessagesNormalCase() {
        final String topicName = "topic-1";
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-111", "value-111"));
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-222", "value-222"));
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-333", "value-333"));

        StringTopicMessage msg1 = logStorage.getMessage(topicName, 0, 0);
        assertMessage("key-111", "value-111", msg1);

        StringTopicMessage msg2 = logStorage.getMessage(topicName, 0, 1);
        assertMessage("key-222", "value-222", msg2);

        StringTopicMessage msg3 = logStorage.getMessage(topicName, 0, 2);
        assertMessage("key-333", "value-333", msg3);
    }

    private static void assertMessage(
            String expectedKey, String expectedValue, StringTopicMessage actualMsg) {
        assertNotNull(actualMsg, "Can't retrieve message");
        assertEquals(expectedKey, actualMsg.key(), "Incorrect key for message");
        assertEquals(expectedValue, actualMsg.value(), "Incorrect value for message");
    }

    @Test
    void getMessageNotExistingTopic() {
        final String topicName = "topic-not-existed-1";

        StringTopicMessage msg = logStorage.getMessage(topicName, 0, 0);

        assertNull(msg, "Retrieved message from not existed topic");
    }

    @Test
    void getMessageExistingTopicNotExistingPartition() {
        final String topicName = "topic-1";
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-111", "value-111"));

        StringTopicMessage msg = logStorage.getMessage(topicName, 1, 0);

        assertNull(msg, "Retrieved message from not existed partition");
    }

    @Test
    void getMessageOutOfBoundOffset() {
        final String topicName = "topic-1";
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-111", "value-111"));

        StringTopicMessage msg = logStorage.getMessage(topicName, 0, 1);

        assertNull(
                msg,
                "Retrieved message for offset '%d', but we only pushed 1 message".formatted(1));
    }

    @Test
    void getMessageNegativeOffset() {
        final String topicName = "topic-1";
        logStorage.appendMessage(topicName, 0, new StringTopicMessage("key-111", "value-111"));

        StringTopicMessage msg = logStorage.getMessage(topicName, 0, -1);

        assertNull(msg, "Retrieved message for negative offset '%d'".formatted(-1));
    }
}
