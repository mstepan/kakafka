package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public class CreateTopicCommand implements Command {

    private String topicName;
    private int partitionsCount;

    public CreateTopicCommand(String topicName, int partitionsCount) {
        this.topicName = topicName;
        this.partitionsCount = partitionsCount;
    }

    public String topicName() {
        return topicName;
    }

    public int partitionsCount() {
        return partitionsCount;
    }

    public CreateTopicCommand(DataIn in) {
        int topicNameLength = in.readInt();
        topicName = in.readString(topicNameLength);
        partitionsCount = in.readInt();
    }

    @Override
    public CommandMarker marker() {
        return CommandMarker.CREATE_TOPIC;
    }

    @Override
    public void encode(DataOut out) {
        out.writeInt(marker().value());
        out.writeString(topicName);
        out.writeInt(partitionsCount);
    }
}
