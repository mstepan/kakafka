package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class CommandResponseDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

        if (in.readableBytes() < Integer.BYTES) {
            return;
        }

        // read marker 'int' value
        int marker = in.readInt();

        if (marker == CommandResponse.GET_METADATA_MARKER) {

            if (in.readableBytes() < Integer.BYTES) {
                return;
            }

            // read 'brokerName' length value
            int brokerNameLength = in.readInt();

            if (in.readableBytes() < brokerNameLength) {
                return;
            }

            String brokerName =
                    in.readCharSequence(brokerNameLength, StandardCharsets.US_ASCII).toString();

            if (in.readableBytes() < Integer.BYTES) {
                return;
            }

            int brokersCount = in.readInt();

            List<LiveBroker> brokers = new ArrayList<>();

            for (int i = 0; i < brokersCount; ++i) {

                if (in.readableBytes() < Integer.BYTES) {
                    return;
                }

                int brokerIdLength = in.readInt();

                if (in.readableBytes() < brokerIdLength) {
                    return;
                }

                String brokerId =
                        in.readCharSequence(brokerIdLength, StandardCharsets.US_ASCII).toString();

                if (in.readableBytes() < Integer.BYTES) {
                    return;
                }

                int brokerUrlLength = in.readInt();

                if (in.readableBytes() < brokerUrlLength) {
                    return;
                }

                String brokerUrl =
                        in.readCharSequence(brokerUrlLength, StandardCharsets.US_ASCII).toString();

                brokers.add(new LiveBroker(brokerId, brokerUrl));
            }
            MetadataState state = new MetadataState(brokerName, brokers);

            out.add(new GetMetadataResponse(state));
        } else {
            throw new IllegalStateException("Unkwown marker type detected: " + marker);
        }
    }
}
