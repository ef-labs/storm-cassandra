/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hmsonline.storm.cassandra.testtools;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.hmsonline.storm.cassandra.client.JsonClusterConfigurator;
import com.hmsonline.storm.cassandra.client.SessionFactory;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.storm.Config;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.BeforeClass;

import static com.hmsonline.storm.cassandra.client.JsonClusterConfigurator.CONFIG_PORT;
import static com.hmsonline.storm.cassandra.client.JsonClusterConfigurator.CONFIG_SEEDS;

public class TestBase {
    private static Logger logger = LoggerFactory.getLogger(TestBase.class);

    public static final String CASSANDRA_CONFIG_KEY = "cassandra-config";
    public String KEYSPACE = this.getClass().getSimpleName().toLowerCase();

    protected static Session session;
    protected static Config stormConfig;

    @BeforeClass
    public static void setupCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(CONFIG_SEEDS, Arrays.asList(EmbeddedCassandraServerHelper.getHost()));
        clientConfig.put(CONFIG_PORT, EmbeddedCassandraServerHelper.getNativeTransportPort());
        stormConfig = new Config();
        stormConfig.put(CASSANDRA_CONFIG_KEY, clientConfig);

        session = SessionFactory.getSingletonSession(CASSANDRA_CONFIG_KEY, () -> new JsonClusterConfigurator(CASSANDRA_CONFIG_KEY, clientConfig));

    }

    @Before
    public void setUp() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // Ignore
        }
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    protected void createTable(String keyspace, String table, Column key, Column... fields) {

        Map<String, Object> replication = new HashMap<>();
        replication.put("class", SimpleStrategy.class.getSimpleName());
        replication.put("replication_factor", 1);
        KeyspaceOptions createKeyspace = SchemaBuilder.createKeyspace(keyspace)
                .ifNotExists()
                .with()
                .replication(replication);
        session.execute(createKeyspace);

        Create createTable = SchemaBuilder.createTable(keyspace, table)
                .addPartitionKey(key.name, key.type);
        Arrays.stream(fields)
                .forEach(k -> createTable.addColumn(k.name, k.type));
        session.execute(createTable);
    }

    protected void createIndex(String keyspace, String table, String field) {
        SchemaStatement createIndex = SchemaBuilder
                .createIndex("index_" + UUID.randomUUID().toString().replace("-", ""))
                .onTable(keyspace, table)
                .andColumn(field);
        session.execute(createIndex);

    }

    protected static Column column(String name, DataType type) {
        Column column = new Column();
        column.name = name;
        column.type = type;
        return column;
    }

    protected static class Column {
        public String name;
        public DataType type;
    }

}
