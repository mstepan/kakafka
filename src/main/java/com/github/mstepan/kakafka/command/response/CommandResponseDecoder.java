package com.github.mstepan.kakafka.command.response;

import com.github.mstepan.kakafka.command.CommandMarker;
import com.github.mstepan.kakafka.io.DataIn;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;

public final class CommandResponseDecoder extends ReplayingDecoder<Void> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) {
        DataIn in = DataIn.fromNettyByteBuf(buf);
        out.add(decode(in));
    }

    public static CommandResponse decode(DataIn in) {
        try {
            // read marker 'int' value
            int typeMarker = in.readInt();

            CommandMarker marker = CommandMarker.fromIntValue(typeMarker);

            return switch (marker) {
                case GET_METADATA -> MetadataCommandResponse.decode(in);
                case EXIT -> ExitCommandResponse.decode(in);
                case CREATE_TOPIC -> CreateTopicCommandResponse.decode(in);
            };

        } catch (Exception ex) {
            throw new IllegalStateException("CommandResponse 'decode' failed", ex);
        }
    }
}
