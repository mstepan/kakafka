package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record CreateTopicCommand(String topicName, int partitionsCount, int replicasCnt)
        implements Command {

    public String topicName() {
        return topicName;
    }

    public int partitionsCount() {
        return partitionsCount;
    }

    @Override
    public void encode(DataOut out) {
        out.writeInt(CommandMarker.CREATE_TOPIC.value());
        out.writeString(topicName);
        out.writeInt(partitionsCount);
        out.writeInt(replicasCnt);
    }

    public static CreateTopicCommand decode(DataIn in) {
        // read 'topicName' string
        String topicName = in.readString();
        int partitionsCount = in.readInt();
        int replicasCount = in.readInt();

        return new CreateTopicCommand(topicName, partitionsCount, replicasCount);
    }
}
