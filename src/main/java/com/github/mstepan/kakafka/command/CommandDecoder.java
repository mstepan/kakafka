package com.github.mstepan.kakafka.command;

import com.github.mstepan.kakafka.io.DataIn;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.util.List;

public final class CommandDecoder extends ReplayingDecoder<Void> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        out.add(decode(DataIn.fromNettyByteBuf(in)));
    }

    public static Command decode(DataIn in) {
        int typeMarker = in.readInt();
        CommandMarker marker = CommandMarker.fromIntValue(typeMarker);

        return switch (marker) {
            case EXIT -> ExitCommand.decode(in);
            case GET_METADATA -> GetMetadataCommand.decode(in);
            case CREATE_TOPIC -> CreateTopicCommand.decode(in);
            case PUSH_MESSAGE -> PushMessageCommand.decode(in);
            case GET_TOPIC_INFO -> GetTopicInfoCommand.decode(in);
            case CONSUME_MESSAGE -> ConsumeMessageCommand.decode(in);
        };
    }
}
