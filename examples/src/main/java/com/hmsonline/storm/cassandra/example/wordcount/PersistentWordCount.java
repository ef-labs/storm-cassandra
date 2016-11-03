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
package com.hmsonline.storm.cassandra.example.wordcount;

import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBolt;
import com.hmsonline.storm.cassandra.mapper.DefaultUpsertMapper;
import com.hmsonline.storm.cassandra.util.CassandraUtils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String CASSANDRA_BOLT = "WORD_COUNT_CASSANDRA_BOLT";

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        CassandraUtils.setCassandraConfig(config, 9160, "localhost");

        TestWordSpout wordSpout = new TestWordSpout();

        TestWordCounter countBolt = new TestWordCounter();

        // create a CassandraBolt that writes to the "stormcf" column
        // family and uses the Tuple field "word" as the row key
        CassandraBolt cassandraBolt = new CassandraBolt()
                .setTupleMapper(new DefaultUpsertMapper("stormks", "stormcf", new Fields("word")))
                .setAckStrategy(AckStrategy.ACK_ON_WRITE);

        // setup topology:
        // wordSpout ==> countBolt ==> cassandraBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, wordSpout, 3);
        builder.setBolt(COUNT_BOLT, countBolt, 3).fieldsGrouping(WORD_SPOUT, new Fields("word"));
        builder.setBolt(CASSANDRA_BOLT, cassandraBolt, 3).shuffleGrouping(COUNT_BOLT);

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}
