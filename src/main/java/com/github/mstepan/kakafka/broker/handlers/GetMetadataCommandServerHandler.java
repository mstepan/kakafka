package com.github.mstepan.kakafka.broker.handlers;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.command.Command;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.response.MetadataCommandResponse;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class GetMetadataCommandServerHandler extends ChannelInboundHandlerAdapter {

    private final String brokerName;

    private final MetadataStorage metadata;

    public GetMetadataCommandServerHandler(String brokerName, MetadataStorage metadata) {
        this.brokerName = brokerName;
        this.metadata = metadata;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        Command command = (Command) msg;

        if (command instanceof GetMetadataCommand) {
            System.out.printf("[%s] 'get_metadata' command received %n", brokerName);
            metadata.getMetadataState().whenComplete(new MetadataListener(ctx, msg));
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private final class MetadataListener implements BiConsumer<GetResponse, Throwable> {

        private final ChannelHandlerContext ctx;
        private final Object msg;

        public MetadataListener(ChannelHandlerContext ctx, Object msg) {
            this.ctx = ctx;
            this.msg = msg;
        }

        @Override
        public void accept(GetResponse getResp, Throwable ex) {
            try {
                if (ex != null) {
                    System.out.printf(
                            "[%s] getting metadata state from 'etcd' FAILED %n", brokerName);
                    ex.printStackTrace();
                    ctx.writeAndFlush(new MetadataCommandResponse(null, 500));
                    return;
                }

                System.out.printf("[%s] metadata state obtained from 'etcd' %n", brokerName);

                List<LiveBroker> liveBrokers = new ArrayList<>();
                for (KeyValue keyValue : getResp.getKvs()) {

                    String brokerIdPath = keyValue.getKey().toString(StandardCharsets.US_ASCII);

                    final String brokerName =
                            brokerIdPath.substring(brokerIdPath.lastIndexOf("/") + 1);
                    final String brokerUrl =
                            keyValue.getValue().toString(StandardCharsets.US_ASCII);

                    liveBrokers.add(new LiveBroker(brokerName, brokerUrl));
                }

                MetadataState state =
                        new MetadataState(metadata.getLeaderBrokerName(), liveBrokers);

                ctx.writeAndFlush(new MetadataCommandResponse(state, 200));
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) throws Exception {
        ex.printStackTrace();
        ctx.close();
    }
}
