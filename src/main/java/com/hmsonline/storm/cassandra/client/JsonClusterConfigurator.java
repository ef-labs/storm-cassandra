package com.hmsonline.storm.cassandra.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class JsonClusterConfigurator implements ClusterConfigurator {

    protected String clusterName;
    protected List<String> seeds;
    protected Integer port;
    protected LoadBalancingPolicy loadBalancingPolicy;
    protected ReconnectionPolicy reconnectionPolicy;
    protected PoolingOptions poolingOptions;
    protected SocketOptions socketOptions;
    protected QueryOptions queryOptions;
    protected MetricsOptions metricsOptions;
    protected AuthProvider authProvider;

    protected final List<String> DEFAULT_SEEDS = ImmutableList.of("127.0.0.1");

    public static final String CONFIG_CLUSTER_NAME = "cluster_name";
    public static final String CONFIG_SEEDS = "seeds";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_POLICIES = "policies";
    public static final String CONFIG_POLICIES_LOAD_BALANCING = "load_balancing";
    public static final String CONFIG_POLICIES_RECONNECTION = "reconnection";
    public static final String CONFIG_POOLING = "pooling";
    public static final String CONFIG_SOCKET = "socket";
    public static final String CONFIG_METRICS = "metrics";
    public static final String CONFIG_AUTH = "auth";
    public static final String CONFIG_QUERY = "query";
    public static final String CONFIG_CONSISTENCY_LEVEL = "consistency_level";
    public static final String CONFIG_SERIAL_CONSISTENCY_LEVEL = "serial_consistency_level";
    public static final String CONFIG_FETCH_SIZE = "fetch_size";

    public static final String CONSISTENCY_ANY = "ANY";
    public static final String CONSISTENCY_ONE = "ONE";
    public static final String CONSISTENCY_TWO = "TWO";
    public static final String CONSISTENCY_THREE = "THREE";
    public static final String CONSISTENCY_QUORUM = "QUORUM";
    public static final String CONSISTENCY_ALL = "ALL";
    public static final String CONSISTENCY_LOCAL_ONE = "LOCAL_ONE";
    public static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    public static final String CONSISTENCY_EACH_QUORUM = "EACH_QUORUM";

    public static final String CONSISTENCY_SERIAL = "SERIAL";
    public static final String CONSISTENCY_LOCAL_SERIAL = "LOCAL_SERIAL";

    public JsonClusterConfigurator(Map<String, Object> config) {
        init(config, null);
    }

    public JsonClusterConfigurator(String clusterName, Map<String, Object> config) {
        init(config, clusterName);
    }

    @Override
    public String clusterName() {
        return clusterName;
    }

    @Override
    public List<String> getSeeds() {
        return seeds;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    @Override
    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    @Override
    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    @Override
    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    @Override
    public MetricsOptions getMetricsOptions() {
        return metricsOptions;
    }

    @Override
    public AuthProvider getAuthProvider() {
        return authProvider;
    }

    @Override
    public PoolingOptions getPoolingOptions() {
        return poolingOptions;
    }

    @Override
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    protected void init(Map<String, Object> config, String clusterName) {

        initSeeds(List.class.cast(config.get(CONFIG_SEEDS)));
        initPort(config);
        initPolicies((Map<String, Object>) config.get(CONFIG_POLICIES));
        initPoolingOptions((Map<String, Object>) config.get(CONFIG_POOLING));
        initSocketOptions((Map<String, Object>) config.get(CONFIG_SOCKET));
        initQueryOptions((Map<String, Object>) config.get(CONFIG_QUERY));
        initMetricsOptions((Map<String, Object>) config.get(CONFIG_METRICS));
        initAuthProvider((Map<String, Object>) config.get(CONFIG_AUTH));
        initClusterName(clusterName, (String) config.get(CONFIG_CLUSTER_NAME));

    }

    private void initClusterName(String nameFromConstructor, String nameFromConfig) {
        this.clusterName = Strings.isNullOrEmpty(nameFromConstructor)
                ? nameFromConfig
                : nameFromConstructor;
    }

    protected void initSeeds(List seeds) {

        // Get array of IPs, default to localhost
        if (seeds == null || seeds.size() == 0) {
            this.seeds = DEFAULT_SEEDS;
            return;
        }

        this.seeds = new ArrayList<>();
        for (int i = 0; i < seeds.size(); i++) {
            this.seeds.add((String) seeds.get(i));
        }
    }

    protected void initPort(Map<String, Object> config) {
        Integer i = ((Number) config.get(CONFIG_PORT)).intValue();

        if (i == null || i <= 0) {
            return;
        }

        port = i;
    }

    protected void initPolicies(Map<String, Object> policyConfig) {

        if (policyConfig == null) {
            return;
        }

        initLoadBalancingPolicy((Map<String, Object>) policyConfig.get(CONFIG_POLICIES_LOAD_BALANCING));
        initReconnectionPolicy((Map<String, Object>) policyConfig.get(CONFIG_POLICIES_RECONNECTION));
    }

    protected void initLoadBalancingPolicy(Map<String, Object> loadBalancing) {

        if (loadBalancing == null) {
            return;
        }

        String name = (String) loadBalancing.get("name");

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A load balancing policy must have a class name field");

        } else if ("DCAwareRoundRobinPolicy".equalsIgnoreCase(name)
                || "com.datastax.driver.core.policies.DCAwareRoundRobinPolicy".equalsIgnoreCase(name)) {

            String localDc = (String) loadBalancing.get("local_dc");
            Number usedHostsPerRemoteDc = (Number) loadBalancing.get("used_hosts_per_remote_dc");

            if (localDc == null || localDc.isEmpty()) {
                throw new IllegalArgumentException("A DCAwareRoundRobinPolicy requires a local_dc in configuration.");
            }

            loadBalancingPolicy = DCAwareRoundRobinPolicy.builder()
                    .withLocalDc(localDc)
                    .withUsedHostsPerRemoteDc(usedHostsPerRemoteDc.intValue())
                    .build();

        } else {

            Class<?> clazz;
            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (LoadBalancingPolicy.class.isAssignableFrom(clazz)) {
                try {
                    loadBalancingPolicy = (LoadBalancingPolicy) clazz.newInstance();
                } catch (IllegalAccessException | InstantiationException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IllegalArgumentException("Class " + name + " does not implement LoadBalancingPolicy");
            }

        }

    }

    protected void initReconnectionPolicy(Map<String, Object> reconnection) {

        if (reconnection == null) {
            return;
        }

        String name = (String) reconnection.get("name");

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A reconnection policy must have a class name field");

        } else if ("ConstantReconnectionPolicy".equalsIgnoreCase(name) || "constant".equalsIgnoreCase(name)) {
            Number delay = (Number) reconnection.get("delay");

            if (delay == null) {
                throw new IllegalArgumentException("ConstantReconnectionPolicy requires a delay in configuration");
            }

            reconnectionPolicy = new ConstantReconnectionPolicy(delay.intValue());

        } else if ("ExponentialReconnectionPolicy".equalsIgnoreCase(name) || "exponential".equalsIgnoreCase(name)) {
            Long baseDelay = (Long) reconnection.get("base_delay");
            Long maxDelay = (Long) reconnection.get("max_delay");

            if (baseDelay == null && maxDelay == null) {
                throw new IllegalArgumentException("ExponentialReconnectionPolicy requires base_delay and max_delay in configuration");
            }

            reconnectionPolicy = new ExponentialReconnectionPolicy(baseDelay.longValue(), maxDelay.longValue());

        } else {
            Class<?> clazz;
            try {
                clazz = Thread.currentThread().getContextClassLoader().loadClass(name);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (!ReconnectionPolicy.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + name + " does not implement ReconnectionPolicy");
            }

            try {
                reconnectionPolicy = (ReconnectionPolicy) clazz.newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }

    }

    protected void initPoolingOptions(Map<String, Object> poolingConfig) {

        if (poolingConfig == null) {
            return;
        }

        poolingOptions = new PoolingOptions();

        Number core_connections_per_host_local = (Number) poolingConfig.get("core_connections_per_host_local");
        Number core_connections_per_host_remote = (Number) poolingConfig.get("core_connections_per_host_remote");
        Number max_connections_per_host_local = (Number) poolingConfig.get("max_connections_per_host_local");
        Number max_connections_per_host_remote = (Number) poolingConfig.get("max_connections_per_host_remote");
        Number new_connection_threshold_local = (Number) poolingConfig.get("new_connection_threshold_local");
        Number new_connection_threshold_remote = (Number) poolingConfig.get("new_connection_threshold_remote");

        if (core_connections_per_host_local != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, core_connections_per_host_local.intValue());
        }
        if (core_connections_per_host_remote != null) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, core_connections_per_host_remote.intValue());
        }
        if (max_connections_per_host_local != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, max_connections_per_host_local.intValue());
        }
        if (max_connections_per_host_remote != null) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, max_connections_per_host_remote.intValue());
        }
        if (new_connection_threshold_local != null) {
            poolingOptions.setNewConnectionThreshold(HostDistance.LOCAL, new_connection_threshold_local.intValue());
        } else {
            Number max_simultaneous_requests_local = (Number) poolingConfig.get("max_simultaneous_requests_local");
            if (max_simultaneous_requests_local != null) {
                poolingOptions.setNewConnectionThreshold(HostDistance.LOCAL, max_simultaneous_requests_local.intValue());
            }
        }
        if (new_connection_threshold_remote != null) {
            poolingOptions.setNewConnectionThreshold(HostDistance.REMOTE, new_connection_threshold_remote.intValue());
        } else {
            Number max_simultaneous_requests_remote = (Number) poolingConfig.get("max_simultaneous_requests_remote");
            if (max_simultaneous_requests_remote != null) {
                poolingOptions.setNewConnectionThreshold(HostDistance.REMOTE, max_simultaneous_requests_remote.intValue());
            }
        }

    }

    protected void initSocketOptions(Map<String, Object> socketConfig) {

        if (socketConfig == null) {
            return;
        }

        socketOptions = new SocketOptions();

        Number connect_timeout_millis = (Number) socketConfig.get("connect_timeout_millis");
        Number read_timeout_millis = (Number) socketConfig.get("read_timeout_millis");
        Boolean keep_alive = (Boolean) socketConfig.get("keep_alive");
        Boolean reuse_address = (Boolean) socketConfig.get("reuse_address");
        Number receive_buffer_size = (Number) socketConfig.get("receive_buffer_size");
        Number send_buffer_size = (Number) socketConfig.get("send_buffer_size");
        Number so_linger = (Number) socketConfig.get("so_linger");
        Boolean tcp_no_delay = (Boolean) socketConfig.get("tcp_no_delay");

        if (connect_timeout_millis != null) {
            socketOptions.setConnectTimeoutMillis(connect_timeout_millis.intValue());
        }
        if (read_timeout_millis != null) {
            socketOptions.setReadTimeoutMillis(read_timeout_millis.intValue());
        }
        if (keep_alive != null) {
            socketOptions.setKeepAlive(keep_alive);
        }
        if (reuse_address != null) {
            socketOptions.setReuseAddress(reuse_address);
        }
        if (receive_buffer_size != null) {
            socketOptions.setReceiveBufferSize(receive_buffer_size.intValue());
        }
        if (send_buffer_size != null) {
            socketOptions.setSendBufferSize(send_buffer_size.intValue());
        }
        if (so_linger != null) {
            socketOptions.setSoLinger(so_linger.intValue());
        }
        if (tcp_no_delay != null) {
            socketOptions.setTcpNoDelay(tcp_no_delay);
        }

    }

    protected void initQueryOptions(Map<String, Object> queryConfig) {

        if (queryConfig == null) {
            return;
        }

        queryOptions = new QueryOptions();

        ConsistencyLevel consistency = getConsistency((String) queryConfig.get(CONFIG_CONSISTENCY_LEVEL));
        if (consistency != null) {
            queryOptions.setConsistencyLevel(consistency);
        }

        ConsistencyLevel serialConsistency = getConsistency((String) queryConfig.get(CONFIG_SERIAL_CONSISTENCY_LEVEL));
        if (serialConsistency != null) {
            queryOptions.setSerialConsistencyLevel(serialConsistency);
        }

        Number fetchSize = (Number) queryConfig.get(CONFIG_FETCH_SIZE);
        if (fetchSize != null) {
            queryOptions.setFetchSize(fetchSize.intValue());
        }

    }

    protected ConsistencyLevel getConsistency(String consistency) {

        if (consistency == null || consistency.isEmpty()) {
            return null;
        }

        if (consistency.equalsIgnoreCase(CONSISTENCY_ANY)) {
            return ConsistencyLevel.ANY;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_ONE)) {
            return ConsistencyLevel.ONE;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_TWO)) {
            return ConsistencyLevel.TWO;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_THREE)) {
            return ConsistencyLevel.THREE;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_QUORUM)) {
            return ConsistencyLevel.QUORUM;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_ALL)) {
            return ConsistencyLevel.ALL;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_LOCAL_ONE)) {
            return ConsistencyLevel.LOCAL_ONE;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_LOCAL_QUORUM)) {
            return ConsistencyLevel.LOCAL_QUORUM;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_EACH_QUORUM)) {
            return ConsistencyLevel.EACH_QUORUM;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_SERIAL)) {
            return ConsistencyLevel.SERIAL;
        }
        if (consistency.equalsIgnoreCase(CONSISTENCY_LOCAL_SERIAL)) {
            return ConsistencyLevel.LOCAL_SERIAL;
        }

        throw new IllegalArgumentException("'" + consistency + "' is not a valid consistency level.");
    }

    protected void initMetricsOptions(Map<String, Object> metrics) {

        if (metrics == null) {
            return;
        }

        boolean enabled = (boolean) metrics.getOrDefault("enabled", true);
        boolean jmx_enabled = (boolean) metrics.getOrDefault("jmx_enabled", true);
        metricsOptions = new MetricsOptions(enabled, jmx_enabled);

    }

    protected void initAuthProvider(Map<String, Object> auth) {

        if (auth == null) {
            return;
        }

        String username = (String) auth.get("username");
        String password = (String) auth.get("password");

//        if (Strings.isNullOrEmpty(username)) {
//            throw new IllegalArgumentException("A username field must be provided on an auth field.");
//        }
//        if (Strings.isNullOrEmpty(password)) {
//            throw new IllegalArgumentException("A password field must be provided on an auth field.");
//        }
        authProvider = new PlainTextAuthProvider(username, password);

    }

}
