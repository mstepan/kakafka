package com.github.mstepan.kakafka.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkLeaderElection {

    private static final String CLUSTER_NAME = "cluster-0";

    private static final String ZK_CONNECTION_STRING = "localhost:2181";
    private static final String LEADER_PATH = "/kakafka/%s/leader".formatted(CLUSTER_NAME);

    public static void main(String[] args) throws Exception {
        // Create a CuratorFramework instance
        CuratorFramework client =
                CuratorFrameworkFactory.newClient(
                        ZK_CONNECTION_STRING, new ExponentialBackoffRetry(1000, 3));
        client.start();

        // Create a LeaderSelector with the client, leader path, and a leader selector listener
        LeaderSelector leaderSelector =
                new LeaderSelector(
                        client,
                        LEADER_PATH,
                        new LeaderSelectorListenerAdapter() {
                            @Override
                            public void takeLeadership(CuratorFramework client) throws Exception {
                                System.out.println(
                                        "This instance is the leader. Perform leader tasks here.");

                                // Keep the leadership until interrupted or an exception occurs
                                //                                Thread.sleep(Long.MAX_VALUE);
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
                        });

        // Start the leader selector
        leaderSelector.start();

        Thread.sleep(30L);

        // Wait for leadership changes
        Thread.sleep(Long.MAX_VALUE);

        // Close the leader selector and the client
        leaderSelector.close();
        client.close();
    }
}
