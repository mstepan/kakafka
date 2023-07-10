package com.github.mstepan.kakafka.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public class ClientMain {

    private static final String HOST = "localhost";

    private static final int PORT = 9091;

    public static void main(String[] args) throws Exception {
        new ClientMain().run(HOST, PORT);
    }

    public void run(String host, int port) throws Exception {
        EventLoopGroup evenLoopGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .group(evenLoopGroup)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(new InetSocketAddress(host, port))
                    .handler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) {
                                    // Add custom handlers
                                    ch.pipeline().addLast(new EchoClientHandler("get_metadata"));
                                }
                            });

            System.out.printf("Client connecting to '%s:%d'%n", host, port);
            ChannelFuture channelFuture = bootstrap.connect().sync();
            channelFuture.channel().closeFuture().sync();

        } finally {
            // shutdown event loop group releasing all resources
            evenLoopGroup.shutdownGracefully().sync();
        }
    }
}
