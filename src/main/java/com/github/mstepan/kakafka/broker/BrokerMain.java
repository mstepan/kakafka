package com.github.mstepan.kakafka.broker;

import com.github.mstepan.kakafka.broker.core.BrokerNameFactory;
import com.github.mstepan.kakafka.broker.core.MetadataStorage;
import com.github.mstepan.kakafka.broker.core.storage.LogStorage;
import com.github.mstepan.kakafka.broker.etcd.EtcdClientHolder;
import com.github.mstepan.kakafka.broker.etcd.KeepAliveAndLeaderElectionTask;
import com.github.mstepan.kakafka.broker.etcd.LiveBrokersTrackerTask;
import com.github.mstepan.kakafka.broker.handlers.ConsumeMessageServerHandler;
import com.github.mstepan.kakafka.broker.handlers.CreateTopicCommandServerHandler;
import com.github.mstepan.kakafka.broker.handlers.ExitCommandServerHandler;
import com.github.mstepan.kakafka.broker.handlers.GetMetadataCommandServerHandler;
import com.github.mstepan.kakafka.broker.handlers.GetTopicInfoServerHandler;
import com.github.mstepan.kakafka.broker.handlers.PushMessageServerHandler;
import com.github.mstepan.kakafka.broker.utils.DaemonThreadFactory;
import com.github.mstepan.kakafka.command.CommandDecoder;
import com.github.mstepan.kakafka.command.response.CommandResponseEncoder;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Watch;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public final class BrokerMain {

    private final BrokerContext brokerCtx;

    public BrokerMain(BrokerContext brokerCtx) {
        this.brokerCtx = brokerCtx;
    }

    /**
     * This group will be used for blocking I/O calls, such as blocking calls to 'etcd' using
     * 'jetcd' client.
     * https://stackoverflow.com/questions/49133447/how-can-you-safely-perform-blocking-operations-in-a-netty-channel-handler
     */
    private static final EventExecutorGroup IO_BLOCKING_ETC_CALLS_GROUP =
            new DefaultEventExecutorGroup(
                    8, new DaemonThreadFactory("netty-io-blocking-etcd-calls-group"));

    private static final EventExecutorGroup IO_BLOCKING_FILE_SYSTEM_CALLS_GROUP =
            new DefaultEventExecutorGroup(
                    8, new DaemonThreadFactory("netty-io-blocking-file-system-calls-group"));

    public static void main(String[] args) throws Exception {
        final BrokerNameFactory nameFac = new BrokerNameFactory();
        final BrokerConfig config =
                new BrokerConfig(
                        nameFac.generateBrokerName(), getPort(), "http://localhost:2379", "./data");
        final MetadataStorage metadataStorage = new MetadataStorage();
        final LogStorage logStorage = new LogStorage(config);
        logStorage.init();

        // jetcd 'Client' and all client classes, like `KV` are thread safe,
        // so we can use one instance per broker.
        try (Client client = Client.builder().endpoints(config.etcdEndpoint()).build();
                Lease leaseClient = client.getLeaseClient();
                Election electionClient = client.getElectionClient();
                KV kvClient = client.getKVClient();
                Watch watchClient = client.getWatchClient()) {

            EtcdClientHolder etcdClientHolder =
                    new EtcdClientHolder(leaseClient, electionClient, kvClient, watchClient);

            BrokerContext brokerContext =
                    new BrokerContext(config, metadataStorage, etcdClientHolder, logStorage);

            new BrokerMain(brokerContext).run(getPort());
        }
    }

    private static int getPort() {
        // read port value from property
        String portAsJavaProp = System.getProperty("broker.port");

        if (portAsJavaProp != null) {
            return Integer.parseInt(portAsJavaProp);
        }

        // read port value from ENV variable
        String portAsEnv = System.getProperty("BROKER_PORT");

        if (portAsEnv != null) {
            return Integer.parseInt(portAsEnv);
        }

        // generate random port to bind if ENV[BROKER_PORT] is not specified
        return 1024 + ThreadLocalRandom.current().nextInt(1024);
    }

    public void run(int port) throws Exception {

        ExecutorService backgroundTasksPool =
                Executors.newFixedThreadPool(2, new DaemonThreadFactory());

        backgroundTasksPool.execute(new KeepAliveAndLeaderElectionTask(brokerCtx));
        backgroundTasksPool.execute(new LiveBrokersTrackerTask(brokerCtx));

        // leak detector
        // https://netty.io/wiki/reference-counted-objects.html
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        // 'boss', accepts an incoming connection
        EventLoopGroup bossGroup = new NioEventLoopGroup();

        // 'worker', handles the traffic of the accepted connection once the boss accepts
        // the connection and registers the accepted connection to the worker.
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) {

                                    final ChannelPipeline pipeline = ch.pipeline();

                                    pipeline.addLast("commandDecoder", new CommandDecoder());

                                    pipeline.addLast(
                                            "commandResponseEncoder", new CommandResponseEncoder());

                                    pipeline.addLast(
                                            "exitHandler",
                                            new ExitCommandServerHandler(
                                                    brokerCtx.config().brokerName()));

                                    pipeline.addLast(
                                            "getMetadataHandler",
                                            new GetMetadataCommandServerHandler(
                                                    brokerCtx.config().brokerName(),
                                                    brokerCtx.metadata()));

                                    pipeline.addLast(
                                            IO_BLOCKING_ETC_CALLS_GROUP,
                                            "getTopicInfo",
                                            new GetTopicInfoServerHandler(
                                                    brokerCtx.config().brokerName(), brokerCtx));

                                    pipeline.addLast(
                                            IO_BLOCKING_FILE_SYSTEM_CALLS_GROUP,
                                            "pushMessageHandler",
                                            new PushMessageServerHandler(
                                                    brokerCtx.config().brokerName(),
                                                    brokerCtx.logStorage()));

                                    pipeline.addLast(
                                            IO_BLOCKING_FILE_SYSTEM_CALLS_GROUP,
                                            "consumeMessageHandler",
                                            new ConsumeMessageServerHandler(
                                                    brokerCtx.config().brokerName(),
                                                    brokerCtx.logStorage()));

                                    pipeline.addLast(
                                            IO_BLOCKING_ETC_CALLS_GROUP,
                                            "createTopicHandler",
                                            new CreateTopicCommandServerHandler(brokerCtx));
                                }
                            })
                    // The number of connections to be queued.
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // Enable TCP keep alive.
                    // Typically, TCP KeepAlive are sent every 45 or 60 seconds on an idle TCP
                    // connection,
                    // and the connection is dropped if 3 sequential ACKs are missed.
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            System.out.printf(
                    "[%s] started at '%s:%d'%n",
                    brokerCtx.config().brokerName(), "localhost", port);

            // Bind and start to accept incoming connections.
            // Bind to the port of all NICs (network interface cards) in the machine.
            ChannelFuture bindFuture = serverBootstrap.bind(port).sync();

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            bindFuture.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            backgroundTasksPool.shutdownNow();
        }
    }
}
