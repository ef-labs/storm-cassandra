package com.hmsonline.storm.cassandra.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class SessionFactory {

    private static Map<String, Cluster> clusters = new ConcurrentHashMap<>();
    private static Map<String, Session> sessions = new ConcurrentHashMap<>();

    public static Session getSingletonSession(ClusterConfigurator configurator) {
        return getSingletonSession(configurator.clusterName(), configurator);
    }

    public static Session getSingletonSession(String clusterName, Supplier<ClusterConfigurator> configurator) {
        Session session = sessions.get(clusterName);
        if (session == null) {
            return getSingletonSession(clusterName, configurator.get());
        }
        else {
            return session;
        }
    }

    private static Session getSingletonSession(String clusterName, ClusterConfigurator configurator) {
        Cluster cluster = clusters.computeIfAbsent(
                clusterName,
                name -> buildCluster(configurator));
        return sessions.computeIfAbsent(
                clusterName,
                name -> cluster.connect());
    }


    private static Cluster buildCluster(ClusterConfigurator configurator) {

        Cluster.Builder clusterBuilder = Cluster.builder();

        // Get array of IPs, default to localhost
        List<String> seeds = configurator.getSeeds();
        if (seeds == null || seeds.isEmpty()) {
            throw new RuntimeException("Cassandra seeds are missing");
        }

        // Add cassandra cluster contact points
        seeds.forEach(clusterBuilder::addContactPoint);

        if (configurator.getPort() != null) {
            clusterBuilder.withPort(configurator.getPort());
        }

        // Add policies to cluster builder
        if (configurator.getLoadBalancingPolicy() != null) {
            clusterBuilder.withLoadBalancingPolicy(configurator.getLoadBalancingPolicy());
        }
        if (configurator.getReconnectionPolicy() != null) {
            clusterBuilder.withReconnectionPolicy(configurator.getReconnectionPolicy());
        }

        // Add pooling options to cluster builder
        if (configurator.getPoolingOptions() != null) {
            clusterBuilder.withPoolingOptions(configurator.getPoolingOptions());
        }

        // Add socket options to cluster builder
        if (configurator.getSocketOptions() != null) {
            clusterBuilder.withSocketOptions(configurator.getSocketOptions());
        }

        if (configurator.getQueryOptions() != null) {
            clusterBuilder.withQueryOptions(configurator.getQueryOptions());
        }

        if (configurator.getMetricsOptions() != null) {
            if (!configurator.getMetricsOptions().isJMXReportingEnabled()) {
                clusterBuilder.withoutJMXReporting();
            }
        }

        if (configurator.getAuthProvider() != null) {
            clusterBuilder.withAuthProvider(configurator.getAuthProvider());
        }

        // Build cluster and connect
        return clusterBuilder.build();

    }

}
