package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.broker.core.topic.TopicPartitionInfo;
import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import java.util.ArrayList;
import java.util.List;

public record CreateTopicCommandResponse(TopicInfo info, int status) implements CommandResponse {

    @Override
    public void encode(DataOut out) {
        // | MARKER, int |
        out.writeInt(CommandMarker.CREATE_TOPIC.value());

        // | status, int |
        out.writeInt(status);

        if (status == 500) {
            return;
        }

        // | partitions size, int |
        out.writeInt(info().partitions().size());

        for (TopicPartitionInfo partitionInfo : info.partitions()) {

            // | partition leader id, string |
            out.writeString(partitionInfo.leader());

            List<String> replicas = partitionInfo.replicas();

            // | replicas count, int |
            out.writeInt(replicas.size());

            for (String singleReplica : replicas) {
                // | single replica id, string |
                out.writeString(singleReplica);
            }
        }
    }

    public static CreateTopicCommandResponse decode(DataIn in) {
        //  | MARKER, int |
        //  will be decoded inside CommandResponseDecoder.decoded

        // | status, int |
        int statusCode = in.readInt();

        if (statusCode == 500) {
            return new CreateTopicCommandResponse(null, statusCode);
        }

        // | partitions size, int |
        int partitionsCount = in.readInt();

        List<TopicPartitionInfo> partitions = new ArrayList<>();

        for (int parId = 0; parId < partitionsCount; ++parId) {

            // | partition leader id string |
            String leader = in.readString();

            // | replicas count, int |
            int replicasCount = in.readInt();

            List<String> replicas = new ArrayList<>();

            for (int repId = 0; repId < replicasCount; ++repId) {

                // | single replica id, string |
                String singleReplicaId = in.readString();

                replicas.add(singleReplicaId);
            }
            partitions.add(new TopicPartitionInfo(leader, replicas));
        }

        return new CreateTopicCommandResponse(new TopicInfo(partitions), statusCode);
    }
}
