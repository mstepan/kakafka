package com.github.mstepan.kakafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.mstepan.kakafka.client.KakafkaClient;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import com.github.mstepan.kakafka.command.response.GetTopicInfoCommandResponse;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class GetTopicInfoIntTest {

    @Test
    void getExistingTopicInfo() {
        try (KakafkaClient client = new KakafkaClient()) {

            final String topicName = "topic-" + System.currentTimeMillis();

            Optional<CreateTopicCommandResponse> maybeCreateResp = client.createTopic(topicName);

            assertTrue(maybeCreateResp.isPresent(), "create topic response is empty");

            assertEquals(
                    200,
                    maybeCreateResp.get().status(),
                    "Incorrect status code for create topic response");

            Optional<GetTopicInfoCommandResponse> maybeTopicInfoResp =
                    client.getTopicInfo(topicName);

            assertTrue(maybeTopicInfoResp.isPresent(), "topic info is empty for existing topic");
        }
    }

    @Test
    void getTopicInfoForNotExistedTopicShouldFail() {
        try (KakafkaClient client = new KakafkaClient()) {

            final String topicName = "topic-not-existed-" + System.currentTimeMillis();

            Optional<GetTopicInfoCommandResponse> maybeTopicInfoResp =
                    client.getTopicInfo(topicName);

            assertTrue(
                    maybeTopicInfoResp.isEmpty(), "topic info is not empty for not existing topic");
        }
    }
}
