package com.github.mstepan.kakafka.broker.command;

import com.github.mstepan.kakafka.dto.KakafkaCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public class CommandDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < Integer.BYTES) {
            return;
        }

        int typeMarker = in.readInt();
        out.add(new KakafkaCommand(KakafkaCommand.Type.fromMarker(typeMarker)));
    }
}
