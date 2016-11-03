package com.hmsonline.storm.cassandra.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;

import java.util.List;

/**
 * Provides cassandra configuration for the session
 */
public interface ClusterConfigurator {

    /**
     * A name for the cluster, used by {@link SessionFactory} to manage sessions.
     * @return the cluster name
     */
    String clusterName();

    /**
     * List of cassandra seed hosts or IPs
     *
     * @return the seed hosts
     */
    List<String> getSeeds();

    /**
     * Optional port to use to connect to cassandra
     *
     * @return the port numer
     */
    Integer getPort();

    /**
     * Optional load balancing policy
     *
     * @return the load balancing policy
     */
    LoadBalancingPolicy getLoadBalancingPolicy();

    /**
     * Optional reconnection policy
     *
     * @return the reconnect policy
     */
    ReconnectionPolicy getReconnectionPolicy();

    /**
     * Optional pooling options
     *
     * @return pooling options
     */
    PoolingOptions getPoolingOptions();

    /**
     * Optional socket options
     *
     * @return socket options
     */
    SocketOptions getSocketOptions();

    /**
     * Optional query options
     *
     * @return query options
     */
    QueryOptions getQueryOptions();

    /**
     * Optional metrics options
     *
     * @return the metrics options
     */
    MetricsOptions getMetricsOptions();

    /**
     * Optional auth provider
     *
     * @return auth provider
     */
    AuthProvider getAuthProvider();

}
