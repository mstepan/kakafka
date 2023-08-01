package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class SimpleClientScenario {

    public static void main(String[] args) {

        try (KakafkaClient client = new KakafkaClient()) {

            Optional<MetadataCommandResponse> maybeMetadataResponse = client.getMetadata();

            if (maybeMetadataResponse.isEmpty()) {
                return;
            }

            MetadataState metaState = maybeMetadataResponse.get().state();
            printMetadata(metaState);

            final String topicName = "topic-" + UUID.randomUUID();

            Optional<TopicInfo> maybeTopicInfo = client.createTopic(topicName);
            maybeTopicInfo.ifPresent(SimpleClientScenario::printTopicInfo);

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            for (int i = 0; i < 3; ++i) {
                final String key = "key-123";
                final String value =
                        "value-%d-%d".formatted(rand.nextInt(1000), rand.nextInt(1000));

                client.pushMessage(topicName, new StringTopicMessage(key, value));
            }
        }
    }

    private static void printMetadata(MetadataState metaState) {
        System.out.println(metaState.asStr());
    }

    private static void printTopicInfo(TopicInfo info) {
        System.out.printf("%nTOPIC INFO%n");
        System.out.printf("topic: %s%n", info.topicName());
        for (TopicPartitionInfo partitionInfo : info.partitions()) {
            System.out.printf("[partition-%d]: %s%n", partitionInfo.idx(), partitionInfo);
        }
    }
}
