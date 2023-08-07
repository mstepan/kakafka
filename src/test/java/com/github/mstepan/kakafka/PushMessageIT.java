package com.github.mstepan.kakafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.client.KakafkaClient;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import com.github.mstepan.kakafka.command.response.PushMessageCommandResponse;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class PushMessageIT {

    @Test
    void pushMessageExistingTopic() {
        try (KakafkaClient client = new KakafkaClient()) {

            final String topicName = "topic-" + System.currentTimeMillis();

            Optional<CreateTopicCommandResponse> maybeResponse = client.createTopic(topicName);

            assertTrue(maybeResponse.isPresent(), "create topic response is empty");

            assertEquals(
                    200,
                    maybeResponse.get().status(),
                    "Incorrect status code for create topic response");

            assertPushSuccess(
                    client.pushMessage(topicName, new StringTopicMessage("key-1", "value-1.1")));
            assertPushSuccess(
                    client.pushMessage(topicName, new StringTopicMessage("key-1", "value-1.2")));

            assertPushSuccess(
                    client.pushMessage(topicName, new StringTopicMessage("key-2", "value-2.1")));
            assertPushSuccess(
                    client.pushMessage(topicName, new StringTopicMessage("key-2", "value-2.2")));

            assertPushSuccess(
                    client.pushMessage(topicName, new StringTopicMessage("key-3", "value-3.1")));
            assertPushSuccess(
                    client.pushMessage(topicName, new StringTopicMessage("key-3", "value-3.2")));
        }
    }

    @Test
    void pushMessageNotExistedTopicShouldFail() {
        try (KakafkaClient client = new KakafkaClient()) {

            final String topicName = "topic-not-exists-push-" + System.currentTimeMillis();

            Optional<PushMessageCommandResponse> resp =
                    client.pushMessage(topicName, new StringTopicMessage("key-1", "value-1"));

            assertTrue(resp.isEmpty(), "push to not existed topic should fail");
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void assertPushSuccess(Optional<PushMessageCommandResponse> resp) {
        assertTrue(resp.isPresent(), "push response is empty");
        assertEquals(200, resp.get().status(), "push response is not 200");
    }
}
