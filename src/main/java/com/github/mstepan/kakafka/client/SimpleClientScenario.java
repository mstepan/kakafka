package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.broker.core.StringTopicMessage;
import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.command.response.ConsumeMessageCommandResponse;
import com.github.mstepan.kakafka.command.response.CreateTopicCommandResponse;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class SimpleClientScenario {

    public static void main(String[] args) {

        try (KakafkaClient client = new KakafkaClient()) {

            // get some cluster metadata
            Optional<MetadataCommandResponse> maybeMetadataResponse = client.getMetadata();

            if (maybeMetadataResponse.isEmpty()) {
                return;
            }

            MetadataState metaState = maybeMetadataResponse.get().state();
            printMetadata(metaState);

            final String topicName = "topic-" + UUID.randomUUID();

            // create new topic
            Optional<CreateTopicCommandResponse> maybeCreateTopicCommandResp =
                    client.createTopic(topicName);
            maybeCreateTopicCommandResp.ifPresent(resp -> printTopicInfo(resp.info()));

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            for (int i = 0; i < 3; ++i) {
                final String key = "key-%d".formatted(rand.nextInt(100));
                final String value =
                        "value-%d-%d".formatted(rand.nextInt(1000), rand.nextInt(1000));

                // push some messages to topic
                client.pushMessage(topicName, new StringTopicMessage(key, value));
            }

            for (int parIdx = 0;
                    parIdx < KakafkaClient.DEFAULT_PARTITIONS_COUNT_PER_TOPIC;
                    ++parIdx) {
                ConsumeMessageCommandResponse consumeMsgResp =
                        client.consumeMessage(topicName, parIdx, 0);
                if (consumeMsgResp.status() == 200) {
                    System.out.printf(
                            "Message received key = '%s', value = '%s'%n",
                            consumeMsgResp.key(), consumeMsgResp.value());
                } else {
                    System.err.printf(
                            "Consume message failed for topic '%s' and partition idx '%d'%n",
                            topicName, parIdx);
                }
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
