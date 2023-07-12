package com.github.mstepan.kakafka.zk;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkLeaderElection {

    private static final String CLUSTER_NAME = "cluster-0";

    private static final String ZK_CONNECTION_STRING = "localhost:2181";
    private static final String LEADER_ELECTION_PATH =
            "/kakafka/%s/election/leader".formatted(CLUSTER_NAME);

    private static final String LEADER_PATH = "/kakafka/%s/leader".formatted(CLUSTER_NAME);

    public static void main(String[] args) throws Exception {

        final String nodeName = "node-%s".formatted(ThreadLocalRandom.current().nextInt(1000));

        // Create a CuratorFramework instance
        try (CuratorFramework mainClient =
                CuratorFrameworkFactory.newClient(
                        ZK_CONNECTION_STRING, new ExponentialBackoffRetry(1000, 3))) {
            mainClient.start();

            // Create a LeaderSelector with the client, leader path, and a leader selector listener
            try (LeaderSelector leaderSelector =
                    new LeaderSelector(
                            mainClient,
                            LEADER_ELECTION_PATH,
                            new LeaderSelectorListenerAdapter() {
                                @Override
                                public void takeLeadership(CuratorFramework client)
                                        throws Exception {
                                    System.out.printf(
                                            "This %s is the leader. Thread-id:%d%n",
                                            nodeName, Thread.currentThread().getId());

                                    // Keep the leadership until interrupted or an exception occurs
                                    //
                                    // client.create().forPath(LEADER_PATH, nodeName.getBytes());

                                    Thread.sleep(Long.MAX_VALUE);
                                }

                                @Override
                                public void stateChanged(
                                        CuratorFramework client, ConnectionState newState) {
                                    if (newState == ConnectionState.LOST
                                            || newState == ConnectionState.SUSPENDED) {
                                        System.out.println(
                                                "This instance lost leadership. Perform cleanup tasks here.");
                                        throw new RuntimeException("Lost leadership. Exiting...");
                                    }
                                }
                            })) {

                // Start the leader selector
                leaderSelector.start();

                TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
            }
        }
    }
}
