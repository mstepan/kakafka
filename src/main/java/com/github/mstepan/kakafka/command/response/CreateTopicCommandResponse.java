package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

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

        info.encode(out);
    }

    public static CreateTopicCommandResponse decode(DataIn in) {
        //  | MARKER, int |
        //  will be decoded inside CommandResponseDecoder.decoded

        // | status, int |
        int statusCode = in.readInt();

        if (statusCode == 500) {
            return new CreateTopicCommandResponse(null, statusCode);
        }

        TopicInfo topicInfo = TopicInfo.decode(in);

        return new CreateTopicCommandResponse(topicInfo, statusCode);
    }
}
