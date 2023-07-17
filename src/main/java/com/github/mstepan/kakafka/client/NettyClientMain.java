package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.LiveBroker;
import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.command.CommandResponseDecoder;
import com.github.mstepan.kakafka.command.KakafkaCommand;
import com.github.mstepan.kakafka.command.KakafkaCommandEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;

public class NettyClientMain {

    private static final String HOST = "localhost";

    private static final int PORT = 9091;

    public static void main(String[] args) throws Exception {
        new NettyClientMain().run(HOST, PORT);
    }

    public void run(String host, int port) throws Exception {

        SynchChannel<MetadataState> metaChannel = new SynchChannel<>();

        // leak detector
        // https://netty.io/wiki/reference-counted-objects.html
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(
                    new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {

                            ch.pipeline()
                                    .addLast(
                                            new KakafkaCommandEncoder(),
                                            new CommandResponseDecoder(),
                                            new CommandClientHandler(metaChannel));
                        }
                    });

            // Start the client.
            ChannelFuture connectFuture = bootstrap.connect(host, port).sync();

            connectFuture.channel().writeAndFlush(KakafkaCommand.metadataCommand()).sync();

            MetadataState state = metaChannel.get();

            System.out.printf("leader broker: %s%n", state.leaderUrl());

            for (LiveBroker broker : state.brokers()) {
                System.out.printf("id: %s, url: %s %n", broker.id(), broker.url());
            }

            connectFuture.channel().writeAndFlush(KakafkaCommand.exitCommand()).sync();

            // Wait until the connection is closed.
            connectFuture.channel().closeFuture().sync();

            System.out.println("Client connection closed");
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
