package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import com.github.mstepan.kakafka.io.DataOut;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public record MetadataCommandResponse(MetadataState state, int statusCode)
        implements CommandResponse {

    @Override
    public void encode(DataOut out) {

        // | MARKER, int |
        out.writeInt(CommandMarker.GET_METADATA.value());

        // | status code, int |
        out.writeInt(statusCode);

        //
        // | <leader broker name length>, int | <leader broker name chars>
        //
        out.writeString(state().leaderBrokerName());

        //
        // | <live brokers count>, int | <broker-1> | ... | <broker-n> |
        //
        Collection<LiveBroker> brokers = state().brokers();
        out.writeInt(brokers.size());

        for (LiveBroker singleBroker : brokers) {
            //
            // | <broker id length>, int | <broker id chars> | <broker url length>, int |
            // <broker url chars |
            //
            out.writeString(singleBroker.id());
            out.writeString(singleBroker.url());
        }
    }

    public static MetadataCommandResponse decode(DataIn in) {
        //  | MARKER, int |
        //  will be decoded inside CommandResponseDecoder.decoded

        // | status code, int |
        int statusCode = in.readInt();

        // read 'brokerName' string
        String brokerName = in.readString();

        // read brokers count
        int brokersCount = in.readInt();

        List<LiveBroker> brokers = new ArrayList<>();

        for (int i = 0; i < brokersCount; ++i) {
            // read 'brokerId' string
            String brokerId = in.readString();

            // read 'broker.url' string
            String brokerUrl = in.readString();

            brokers.add(new LiveBroker(brokerId, brokerUrl));
        }
        return new MetadataCommandResponse(new MetadataState(brokerName, brokers), statusCode);
    }
}
