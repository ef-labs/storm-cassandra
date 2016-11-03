package com.hmsonline.storm.cassandra.trident;

import com.datastax.driver.core.Session;
import com.hmsonline.storm.cassandra.Constants;
import com.hmsonline.storm.cassandra.client.JsonClusterConfigurator;
import com.hmsonline.storm.cassandra.client.SessionFactory;
import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;
import com.hmsonline.storm.cassandra.exceptions.MappedExceptionHandler;
import com.hmsonline.storm.cassandra.util.CassandraExecutor;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class CassandraMapStateFactory implements StateFactory {

    public static final String DEFAULT_CASSANDRA_CONFIG_KEY = Constants.DEFAULT_CASSANDRA_CONFIG_KEY;

    private static final long serialVersionUID = 1867683810551239086L;
    private static final Logger logger = LoggerFactory.getLogger(CassandraMapStateFactory.class);
    private static final ExecutorService executor = Executors.newWorkStealingPool();

    private String clientConfigKey = DEFAULT_CASSANDRA_CONFIG_KEY;
    private String stateField;
    private String keyspace;
    private String table;
    private String[] keyNames;
    private Serializer serializer;
    private int cacheSize = 0;
    private StateType stateType;
    private Integer maxParallelism = 0;
    private ExceptionHandler exceptionHandler;

    private Semaphore throttle;

    private <T> CassandraMapStateFactory(StateType stateType, Serializer serializer) {
        this.stateType = stateType;
        this.serializer = serializer;
    }

    public static <T> CassandraMapStateFactory opaque() {
        return new CassandraMapStateFactory(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    public static <T> CassandraMapStateFactory opaque(Serializer<OpaqueValue<T>> serializer) {
        return new CassandraMapStateFactory(StateType.OPAQUE, serializer);
    }

    public static <T> CassandraMapStateFactory transactional() {
        return new CassandraMapStateFactory(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
    }

    public static <T> CassandraMapStateFactory transactional(Serializer<TransactionalValue<T>> serializer) {
        return new CassandraMapStateFactory(StateType.TRANSACTIONAL, serializer);
    }

    public static <T> CassandraMapStateFactory nonTransactional() {
        return new CassandraMapStateFactory(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
    }

    public static <T> CassandraMapStateFactory nonTransactional(Serializer<T> serializer) {
        return new CassandraMapStateFactory(StateType.NON_TRANSACTIONAL, serializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        @SuppressWarnings("unchecked")
        Session session = SessionFactory.getSingletonSession(
                clientConfigKey,
                () -> new JsonClusterConfigurator((Map<String, Object>) conf.get(clientConfigKey)));

        if (exceptionHandler == null) {
            exceptionHandler = new MappedExceptionHandler();
        }

        logger.info("Creating state for {}:{} with keys {} and value {}, serializer {}",
                keyspace, table, keyNames, stateField, serializer
        );

        CassandraExecutor cassandraExecutor = new CassandraExecutor(session, executor, maxParallelism);
        IBackingMap<?> cassandraBackingMap = new CassandraBackingMap(cassandraExecutor, keyspace, table, keyNames, stateField, serializer, exceptionHandler);

        IBackingMap backingMap = cacheSize > 0
                ? new CachedMap<>(cassandraBackingMap, cacheSize)
                : cassandraBackingMap;

        MapState<?> mapState;

        switch (stateType) {
            case OPAQUE:
                mapState = OpaqueMap.build(backingMap);
                break;

            case TRANSACTIONAL:
                mapState = TransactionalMap.build(backingMap);
                break;

            case NON_TRANSACTIONAL:
                mapState = NonTransactionalMap.build(backingMap);
                break;

            default:
                throw new IllegalArgumentException("Invalid state provided " + stateType);
        }

        return mapState;
    }

    public String getStateField() {
        return stateField;
    }

    public CassandraMapStateFactory setStateField(String stateField) {
        this.stateField = stateField;
        return this;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public CassandraMapStateFactory setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    public String getTable() {
        return table;
    }

    public CassandraMapStateFactory setTable(String table) {
        this.table = table;
        return this;
    }

    public String[] getKeyNames() {
        return keyNames;
    }

    public CassandraMapStateFactory setKeyFields(String... keyNames) {
        this.keyNames = keyNames;
        return this;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public String getClientConfigKey() {
        return this.clientConfigKey;
    }

    public CassandraMapStateFactory setCassandraConfigKey(String clientConfigKey) {
        this.clientConfigKey = clientConfigKey;
        return this;
    }

    public CassandraMapStateFactory setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public Integer getMaxParallelism() {
        return maxParallelism;
    }

    public CassandraMapStateFactory setMaxParallelism(Integer maxParallelism) {
        this.maxParallelism = maxParallelism;
        return this;
    }

    public CassandraMapStateFactory setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }
}

