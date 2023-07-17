package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.broker.core.MetadataState;
import com.github.mstepan.kakafka.command.CommandEncoder;
import com.github.mstepan.kakafka.command.ExitCommand;
import com.github.mstepan.kakafka.command.GetMetadataCommand;
import com.github.mstepan.kakafka.command.response.CommandResponseDecoder;
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
                                            new CommandEncoder(),
                                            new CommandResponseDecoder(),
                                            new CommandClientHandler(metaChannel));
                        }
                    });

            // Start the client.
            ChannelFuture connectFuture = bootstrap.connect(host, port).sync();

            connectFuture.channel().writeAndFlush(new GetMetadataCommand()).sync();

            MetadataState state = metaChannel.get();

            System.out.println(state.asStr());

            connectFuture.channel().writeAndFlush(new ExitCommand()).sync();

            // Wait until the connection is closed.
            connectFuture.channel().closeFuture().sync();

            System.out.println("Client connection closed");
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
