package com.github.mstepan.kakafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.client.KakafkaClient;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class CreateTopicIntTest {

    @Test
    public void createNewTopic() {
        try (KakafkaClient client = new KakafkaClient()) {

            final String topicName = "topic-" + System.currentTimeMillis();

            Optional<CreateTopicCommandResponse> maybeResponse = client.createTopic(topicName);

            assertTrue(maybeResponse.isPresent(), "create topic response is empty");

            assertEquals(
                    200,
                    maybeResponse.get().status(),
                    "Incorrect status code for create topic response");

            TopicInfo info = maybeResponse.get().info();

            assertEquals(topicName, info.topicName(), "Incorrect topic name from response");

            List<TopicPartitionInfo> partitions = info.partitions();

            assertEquals(
                    KakafkaClient.DEFAULT_PARTITIONS_COUNT_PER_TOPIC,
                    partitions.size(),
                    "Incorrect number of partitions");

            for (TopicPartitionInfo singlePartition : partitions) {
                assertNotNull(singlePartition.leader(), "null 'leader' detected for partition");

                // The replicas count should be one less than
                // 'DEFAULT_REPLICATION_FACTOR_FOR_SINGLE_PARTITION'
                // because leader will be also counted as a replica
                assertEquals(
                        KakafkaClient.DEFAULT_REPLICATION_FACTOR_FOR_SINGLE_PARTITION - 1,
                        singlePartition.replicas().size(),
                        "incorrect replication factor detected");
            }
        }
    }

    @Test
    void createAlreadyExistingTopicShouldFail() {
        try (KakafkaClient client = new KakafkaClient()) {

            final String topicName = "topic-" + System.currentTimeMillis();

            // 1st call to create topic will succeed
            Optional<CreateTopicCommandResponse> maybeResponse1 = client.createTopic(topicName);

            assertTrue(maybeResponse1.isPresent(), "create topic response is empty");

            assertEquals(
                    200,
                    maybeResponse1.get().status(),
                    "Incorrect status code for create topic response");

            // 2nd call for the same topic name should fail
            Optional<CreateTopicCommandResponse> maybeResponse2 = client.createTopic(topicName);

            assertTrue(
                    maybeResponse2.isEmpty(),
                    "Create topic with same name should return empty response");
        }
    }
}
