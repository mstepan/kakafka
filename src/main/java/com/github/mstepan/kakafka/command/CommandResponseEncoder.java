package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class CommandResponseEncoder extends MessageToByteEncoder<CommandResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, CommandResponse msg, ByteBuf out) {

        if (msg instanceof GetMetadataResponse metadataResp) {

            //
            // | MARKER, int | <leader broker name length>, int | <leader broker name chars>
            //
            out.writeInt(CommandResponse.GET_METADATA_MARKER);

            writeString(out, metadataResp.state().leaderBrokerName());

            //
            // | <live brokers count>, int | <broker-1> | ... | <broker-n> |
            //
            List<LiveBroker> brokers = metadataResp.state().brokers();
            out.writeInt(brokers.size());

            for (LiveBroker singleBroker : brokers) {
                //
                // | <broker id length>, int | <broker id chars> | <broker url length>, int |
                // <broker url chars |
                //
                writeString(out, singleBroker.id());
                writeString(out, singleBroker.url());
            }
        }
    }

    private static void writeString(ByteBuf out, String value) {
        out.writeInt(value.length());
        out.writeCharSequence(value, StandardCharsets.US_ASCII);
    }
}
