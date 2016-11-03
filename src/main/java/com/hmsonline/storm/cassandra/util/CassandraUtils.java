package com.hmsonline.storm.cassandra.util;

import com.datastax.driver.core.*;
import com.hmsonline.storm.cassandra.exceptions.MultiException;
import com.hmsonline.storm.cassandra.exceptions.RecoverableException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hmsonline.storm.cassandra.Constants.DEFAULT_CASSANDRA_CONFIG_KEY;
import static com.hmsonline.storm.cassandra.client.JsonClusterConfigurator.CONFIG_PORT;
import static com.hmsonline.storm.cassandra.client.JsonClusterConfigurator.CONFIG_SEEDS;

/**
 * Utility class
 */
public class CassandraUtils {

    public static String getStatementCacheKey(
            String name,
            String keyspace,
            String table,
            List<String> bindingFields
    ) {
        return getStatementCacheKey(name, keyspace, table, bindingFields, null);
    }

    public static String getStatementCacheKey(
            String name,
            String keyspace,
            String table,
            Iterable<String> bindingFields,
            Iterable<String> resultFields) {

        StringBuilder cacheKey = new StringBuilder();
        cacheKey.append(name)
                .append(':')
                .append(keyspace)
                .append(':')
                .append(table)
                .append(':');

        if (resultFields != null) {
            bindingFields.forEach(f -> {
                cacheKey.append(f)
                        .append(",");
            });
        }

        if (resultFields != null) {
            resultFields.forEach(f -> {
                cacheKey.append(f)
                        .append(",");
            });
        }
        return cacheKey.toString();
    }

    public static Map<String, Object> setCassandraConfig(Map<String, Object> stormConfig, Integer port, String... hosts) {
        return setCassandraConfig(stormConfig, DEFAULT_CASSANDRA_CONFIG_KEY, port, hosts);
    }

    public static Map<String, Object> setCassandraConfig(Map<String, Object> stormConfig, String cassandraConfigKey, Integer port, String... hosts) {
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(CONFIG_SEEDS, Arrays.asList(hosts));
        clientConfig.put(CONFIG_PORT, port);
        stormConfig.put(cassandraConfigKey, clientConfig);
        return clientConfig;
    }

}
