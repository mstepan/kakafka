package com.github.mstepan.kakafka.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadLocalRandom;

public class BrokerMain {

    public static void main(String[] args) throws Exception {


        new BrokerMain().run(getPort());
    }

    private static int getPort() {
        String portAsEnv = System.getenv("BROKER_PORT");

        if (portAsEnv != null) {
            return Integer.parseInt(portAsEnv);
        }

        return 1024 + ThreadLocalRandom.current().nextInt(1024);
    }

    public void run(int port) throws Exception {
        EventLoopGroup evenLoopGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap server = new ServerBootstrap();
            server.group(evenLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(port))
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) {
                                    // Add custom handlers
                                    ch.pipeline().addLast(new EchoServerHandler());
                                }
                            });

            System.out.printf("Starting broker at '%s:%d'%n", "localhost", port);
            ChannelFuture serverBindFuture = server.bind().sync();
            serverBindFuture.channel().closeFuture().sync();

        } finally {
            // shutdown event loop group releasing all resoirces
            evenLoopGroup.shutdownGracefully().sync();
        }
    }
}
