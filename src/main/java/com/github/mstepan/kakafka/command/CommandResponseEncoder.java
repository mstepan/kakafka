package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.io.DataOut;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.util.List;

public final class CommandResponseEncoder extends MessageToByteEncoder<CommandResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, CommandResponse msg, ByteBuf buf) {

        DataOut out = DataOut.fromNettyByteBuf(buf);

        if (msg instanceof GetMetadataResponse metadataResp) {

            //
            // | MARKER, int | <leader broker name length>, int | <leader broker name chars>
            //
            out.writeInt(CommandResponse.GET_METADATA_MARKER);

            out.writeString(metadataResp.state().leaderBrokerName());

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
                out.writeString(singleBroker.id());
                out.writeString(singleBroker.url());
            }
        }
    }
}
