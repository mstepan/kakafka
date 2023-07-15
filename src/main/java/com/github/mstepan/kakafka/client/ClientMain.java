package com.github.mstepan.kakafka.client;

import com.github.mstepan.kakafka.client.command.CommandEncoder;
import com.github.mstepan.kakafka.client.command.CommandResponseDecoder;
import com.github.mstepan.kakafka.client.command.SendCommandRequestHandler;
import com.github.mstepan.kakafka.dto.KakafkaCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ClientMain {

    private static final String HOST = "localhost";

    private static final int PORT = 9091;

    public static void main(String[] args) throws Exception {
        new ClientMain().run(HOST, PORT);
    }

    public void run(String host, int port) throws Exception {
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
                                            new SendCommandRequestHandler());
                        }
                    });

            // Start the client.
            ChannelFuture connectFuture = bootstrap.connect(host, port).sync();

            connectFuture.channel().writeAndFlush(KakafkaCommand.metadataCommand()).sync();

            connectFuture.channel().writeAndFlush(KakafkaCommand.exitCommand()).sync();

            // Wait until the connection is closed.
            connectFuture.channel().closeFuture().sync();

            System.out.println("Client connection closed");
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
