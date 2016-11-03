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

package com.hmsonline.storm.cassandra.trident;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.hmsonline.storm.cassandra.testtools.TestBase;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CassandraBackingMapTest extends TestBase {

    private static Logger logger = LoggerFactory.getLogger(CassandraBackingMapTest.class);

    @Test
    public void nonTransactionalStateTest() throws InterruptedException {
        String stateTable = "words_state";
        String key = "word";
        String state = "state";

        CassandraMapStateFactory factory = CassandraMapStateFactory.nonTransactional()
                .setKeyspace(KEYSPACE)
                .setTable(stateTable)
                .setKeyFields(key)
                .setStateField(state);

        wordsTest(factory, stateTable, key, state);
    }

    @Test
    public void transactionalStateTest() throws InterruptedException {
        String stateTable = "words_state";
        String key = "word";
        String state = "state";

        CassandraMapStateFactory factory = CassandraMapStateFactory.transactional()
                .setKeyspace(KEYSPACE)
                .setTable(stateTable)
                .setKeyFields(key)
                .setStateField(state);

        wordsTest(factory, stateTable, key, state);
    }

    @Test
    public void opaqueStateTest() throws InterruptedException {
        String stateTable = "words_state";
        String key = "word";
        String state = "state";

        CassandraMapStateFactory factory = CassandraMapStateFactory.opaque()
                .setKeyspace(KEYSPACE)
                .setTable(stateTable)
                .setKeyFields(key)
                .setStateField(state);

        wordsTest(factory, stateTable, key, state);
    }

    public void wordsTest(StateFactory factory, String stateTable, String key, String state) throws InterruptedException {

        createTable(KEYSPACE, stateTable,
                column(key, DataType.varchar()),
                column(state, DataType.blob()));

        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();

        TridentState wordCounts = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
                .persistentAggregate(factory, new Count(), new Fields("count")).parallelismHint(1);

        LocalDRPC client = new LocalDRPC();
        topology.newDRPCStream("words", client)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        LocalCluster cluster = new LocalCluster();
        logger.info("Submitting topology.");
        cluster.submitTopology("test", stormConfig, topology.build());

        logger.info("Waiting for something to happen.");
        do {
            Thread.sleep(500);
        } while (session.execute(QueryBuilder.select().all().from(KEYSPACE, stateTable))
                .getAvailableWithoutFetching() < 18);

        logger.info("Starting queries.");
        assertEquals("[[5]]", client.execute("words", "cat dog the man")); // 5
        assertEquals("[[0]]", client.execute("words", "cat")); // 0
        assertEquals("[[0]]", client.execute("words", "dog")); // 0
        assertEquals("[[4]]", client.execute("words", "the")); // 4
        assertEquals("[[1]]", client.execute("words", "man")); // 1

        cluster.shutdown();

    }

}

