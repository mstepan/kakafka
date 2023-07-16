package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.ReplayingDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class CommandResponseDecoder extends ReplayingDecoder<Void> {

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

            // read brokers count
            int brokersCount = in.readInt();

            List<LiveBroker> brokers = new ArrayList<>();

            for (int i = 0; i < brokersCount; ++i) {

                // TODO: below code won't work during fragmentation
                if (in.readableBytes() < Integer.BYTES) {
                    System.out.println("RET-1");
                    return;
                }

                // read brokerId.length
                int brokerIdLength = in.readInt();

                if (in.readableBytes() < brokerIdLength) {
                    System.out.println("RET-2");
                    return;
                }

                // read 'brokerId' chars
                String brokerId =
                        in.readCharSequence(brokerIdLength, StandardCharsets.US_ASCII).toString();

                if (in.readableBytes() < Integer.BYTES) {
                    System.out.println("RET-3");
                    return;
                }

                // read 'broker.url' length
                int brokerUrlLength = in.readInt();

                if (in.readableBytes() < brokerUrlLength) {
                    System.out.println("RET-4");
                    return;
                }

                // read 'broker.url' chars
                String brokerUrl =
                        in.readCharSequence(brokerUrlLength, StandardCharsets.US_ASCII).toString();

                brokers.add(new LiveBroker(brokerId, brokerUrl));
            }
            out.add(new GetMetadataResponse(new MetadataState(brokerName, brokers)));
        } else {
            throw new IllegalStateException("Unknown marker type detected: " + marker);
        }
    }
}
