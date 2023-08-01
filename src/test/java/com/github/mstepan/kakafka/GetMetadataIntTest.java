package com.github.mstepan.kakafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.client.KakafkaClient;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class GetMetadataIntTest {

    @Test
    public void getMetadataNormalCase() {
        try (KakafkaClient client = new KakafkaClient()) {

            Optional<MetadataCommandResponse> maybeMetadataResponse = client.getMetadata();
            assertTrue(maybeMetadataResponse.isPresent(), "metadata is empty");

            MetadataCommandResponse resp = maybeMetadataResponse.get();

            assertEquals(200, resp.statusCode(), "Incorrect status code for metadata response");

            MetadataState metaState = resp.state();

            assertFalse(metaState.brokers().isEmpty(), "metadata live brokers are empty");

            assertNotNull(metaState.leaderBrokerName(), "LEADER broker is null in metadata");
        }
    }
}
