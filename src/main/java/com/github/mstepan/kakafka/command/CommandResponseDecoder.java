package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.io.DataIn;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.ArrayList;
import java.util.List;

public final class CommandResponseDecoder extends ReplayingDecoder<Void> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) {
        DataIn in = DataIn.fromNettyByteBuf(buf);
        out.add(decode(in));
    }

    public static CommandResponse decode(DataIn in) {
        // read marker 'int' value
        int marker = in.readInt();

        if (marker == CommandResponse.GET_METADATA_MARKER) {

            // read 'brokerName' length value
            int brokerNameLength = in.readInt();

            String brokerName = in.readString(brokerNameLength);

            // read brokers count
            int brokersCount = in.readInt();

            List<LiveBroker> brokers = new ArrayList<>();

            for (int i = 0; i < brokersCount; ++i) {

                // read brokerId.length
                int brokerIdLength = in.readInt();

                // read 'brokerId' chars
                String brokerId = in.readString(brokerIdLength);

                // read 'broker.url' length
                int brokerUrlLength = in.readInt();

                // read 'broker.url' chars
                String brokerUrl = in.readString(brokerUrlLength);

                brokers.add(new LiveBroker(brokerId, brokerUrl));
            }
            return new GetMetadataResponse(new MetadataState(brokerName, brokers));
        } else {
            throw new IllegalStateException("Unknown marker type detected: " + marker);
        }
    }
}
