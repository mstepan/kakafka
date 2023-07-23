package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.broker.core.topic.TopicInfo;
import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;

public record GetTopicInfoCommandResponse(TopicInfo info, int status) implements CommandResponse {

    @Override
    public void encode(DataOut out) {
        // | MARKER, int |
        out.writeInt(CommandMarker.GET_TOPIC_INFO.value());

        // | status, int |
        out.writeInt(status);

        if (status != 200) {
            return;
        }

        info.encode(out);
    }

    public static GetTopicInfoCommandResponse decode(DataIn in) {
        //  | MARKER, int |
        //  will be decoded inside CommandResponseDecoder.decoded

        // | status, int |
        int status = in.readInt();

        if (status != 200) {
            return new GetTopicInfoCommandResponse(null, status);
        }

        TopicInfo topicInfo = TopicInfo.decode(in);

        return new GetTopicInfoCommandResponse(topicInfo, status);
    }
}
