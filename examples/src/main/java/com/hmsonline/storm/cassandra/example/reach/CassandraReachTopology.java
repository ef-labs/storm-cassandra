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

package com.hmsonline.storm.cassandra.example.reach;

import com.hmsonline.storm.cassandra.client.JsonClusterConfigurator;
import com.hmsonline.storm.cassandra.client.SessionFactory;
import com.hmsonline.storm.cassandra.mapper.DefaultLookupMapper;
import com.hmsonline.storm.cassandra.trident.CassandraLookupFunction;
import com.hmsonline.storm.cassandra.util.CassandraUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static com.hmsonline.storm.cassandra.Constants.DEFAULT_CASSANDRA_CONFIG_KEY;

@SuppressWarnings("deprecation")
public class CassandraReachTopology {

    private static final Logger logger = LoggerFactory.getLogger(CassandraReachTopology.class);

    public static final String STORM_KEYSPACE = "stormks";
    public static final String TWEETERS = "tweeters";
    public static final String FOLLOWERS = "followers";

    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
    }};

    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
    }};

    public static void main(String[] args) throws Exception {

        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        Config config = new Config();
        CassandraUtils.setCassandraConfig(
                config,
                EmbeddedCassandraServerHelper.getNativeTransportPort(),
                EmbeddedCassandraServerHelper.getHost());

        addTestRecords(config);

        logger.info("Cassandra data in place, constructing topology.");
        LocalDRPC drpc = new LocalDRPC();
        StormTopology topology = construct(drpc);

        if (args == null || args.length == 0) {
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            if ("true".equals(System.getProperty("debug"))) {
                config.setDebug(true);
            }

            cluster.submitTopology("stormreach-drpc", config, topology);

            String[] urisToTry = new String[]{
                    "foo.com/blog/1",
                    "engineering.twitter.com/blog/5",
                    "tech.backtype.com/blog/123"
            };

            for (String uri : urisToTry) {
                logger.info("Reach of {}: {}", uri, drpc.execute("reach", uri));
            }

            cluster.shutdown();
            drpc.shutdown();
        } else {
            config.setNumWorkers(6);
            StormSubmitter.submitTopology(args[0], config, topology);
        }

        // Cassandra unit does not have a stop method, so just shut down the process here.
        System.exit(0);

    }

    public static StormTopology construct(LocalDRPC drpc) {

        TridentTopology topology = new TridentTopology();

        CassandraLookupFunction getTweeters = new CassandraLookupFunction()
                .setLookupMapper(new DefaultLookupMapper(STORM_KEYSPACE, TWEETERS, new Fields("uri"), new Fields("tweeter")));

        CassandraLookupFunction getFollowers = new CassandraLookupFunction()
                .setLookupMapper(new DefaultLookupMapper(STORM_KEYSPACE, FOLLOWERS, new Fields("tweeter"), new Fields("follower")));

        topology.newDRPCStream("reach", drpc)
                .each(new Fields("args"), new Split(), new Fields("uri"))
                .each(new Fields("uri"), getTweeters, new Fields("tweeter"))
                .each(new Fields("tweeter"), getFollowers, new Fields("follower"))
                .groupBy(new Fields("uri"))
                .aggregate(new Count(), new Fields("count"));

        return topology.build();
    }

    private static void addTestRecords(Config config) {
        Session session = SessionFactory.getSingletonSession(
                DEFAULT_CASSANDRA_CONFIG_KEY,
                () -> new JsonClusterConfigurator(
                        DEFAULT_CASSANDRA_CONFIG_KEY,
                        (Map<String, Object>) config.get(DEFAULT_CASSANDRA_CONFIG_KEY)));

        // create the keyspace
        Map<String, Object> replication = new HashMap<>();
        replication.put("class", SimpleStrategy.class.getSimpleName());
        replication.put("replication_factor", 1);
        KeyspaceOptions createKeyspace = SchemaBuilder.createKeyspace(STORM_KEYSPACE)
                .ifNotExists()
                .with()
                .replication(replication);
        session.execute(createKeyspace);

        Create createTweetersTable = SchemaBuilder.createTable(STORM_KEYSPACE, TWEETERS)
                .ifNotExists()
                .addPartitionKey("uri", DataType.varchar())
                .addClusteringColumn("tweeter", DataType.varchar());
        session.execute(createTweetersTable);

        Create createFollowersTable = SchemaBuilder.createTable(STORM_KEYSPACE, FOLLOWERS)
                .ifNotExists()
                .addPartitionKey("tweeter", DataType.varchar())
                .addClusteringColumn("follower", DataType.varchar());
        session.execute(createFollowersTable);

        // Insert data
        TWEETERS_DB.forEach((uri, tweeters) -> {
            tweeters.forEach(tweeter -> {
                session.execute(QueryBuilder
                        .insertInto(STORM_KEYSPACE, TWEETERS)
                        .values(
                                Arrays.asList("uri", "tweeter"),
                                Arrays.asList(uri, tweeter)
                        ));
            });
        });

        FOLLOWERS_DB.forEach((tweeter, followers) -> {
            followers.forEach(follower -> {
                session.execute(QueryBuilder
                        .insertInto(STORM_KEYSPACE, FOLLOWERS)
                        .values(
                                Arrays.asList("tweeter", "follower"),
                                Arrays.asList(tweeter, follower)
                        ));
            });
        });

    }

}
