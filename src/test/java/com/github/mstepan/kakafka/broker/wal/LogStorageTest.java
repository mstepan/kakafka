package com.github.mstepan.kakafka.broker.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.mstepan.kakafka.broker.BrokerConfig;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.storage.LogStorage;
import com.github.mstepan.kakafka.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        logStorage.close();
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

    @Test
    void addMessagesConcurrently() throws Exception {
        final String topicName = "topic-1";
        final String msgKey = "key-111";
        final int messagesPerThread = 100;

        final int threadsCount = 10;
        final CountDownLatch allCompleted = new CountDownLatch(threadsCount);

        ExecutorService pool = Executors.newFixedThreadPool(threadsCount);
        try {
            for (int i = 0; i < threadsCount; ++i) {
                final int threadIdx = i;
                pool.execute(
                        () -> {
                            try {
                                for (int it = 0;
                                        it < messagesPerThread
                                                && !Thread.currentThread().isInterrupted();
                                        ++it) {
                                    logStorage.appendMessage(
                                            topicName,
                                            0,
                                            new StringTopicMessage(msgKey, "value-" + threadIdx));
                                }
                            } finally {
                                allCompleted.countDown();
                            }
                        });
            }

            // wait till all threads will complete publishing messages
            allCompleted.await();
        } finally {
            pool.shutdownNow();
        }

        final Set<String> expectedValues = new HashSet<>();

        for (int thId = 0; thId < threadsCount; ++thId) {
            expectedValues.add("value-" + thId);
        }

        for (int msgIdx = 0; msgIdx < threadsCount * messagesPerThread; ++msgIdx) {
            StringTopicMessage msg = logStorage.getMessage(topicName, 0, msgIdx);

            assertNotNull(msg, "Can't retrieve message");
            assertEquals(msgKey, msg.key(), "Incorrect key for message");
            assertTrue(
                    expectedValues.contains(msg.value()),
                    "Message value is not as expected, expected one of %s, found = '%s'"
                            .formatted(expectedValues, msg.value()));
        }
    }
}
