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
package com.hmsonline.storm.cassandra.bolt;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.hmsonline.storm.cassandra.mapper.DefaultLookupMapper;
import com.hmsonline.storm.cassandra.testtools.TestBase;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.apache.storm.utils.Utils.DEFAULT_STREAM_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CassandraLookupBoltTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(CassandraLookupBoltTest.class);

    private static ExecutorService executor = Executors.newCachedThreadPool();

    private static final String lookupTable = "lookup";
    private static final String keyField = "i1";

    @Mock
    private OutputCollector collector;

    @Mock
    private TopologyContext context;

    @Override
    public void setUp() {
        super.setUp();
        createTable(KEYSPACE, lookupTable,
                column(keyField, DataType.cint()),
                column("i2", DataType.cint()),
                column("i3", DataType.cint()),
                column("i4", DataType.cint()),
                column("i5", DataType.cint()),
                column("i6", DataType.cint())
        );

        createIndex(KEYSPACE, lookupTable, "i6");

    }

    @Test
    public void CassandraLookupBolt_SingleValueTest() throws Exception {

        session.execute(QueryBuilder.insertInto(KEYSPACE, lookupTable)
                .values(
                        Arrays.asList(keyField, "i2", "i3", "i4", "i5", "i6"),
                        Arrays.asList(4, 8, 15, 16, 23, 42)
                )
        );

        logger.info("Preparing bolt.");
        CassandraLookupBolt bolt = new CassandraLookupBolt()
                .setLookupMapper(new DefaultLookupMapper(
                        KEYSPACE,
                        lookupTable,
                        new Fields(keyField),
                        new Fields("i6")));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);

        when(context.getSharedExecutor())
                .thenReturn(executor);
        when(context.getThisOutputFieldsForStreams())
                .thenReturn(getOutputFieldsForStreams("i6"));
        when(context.getComponentOutputFields(any(), any()))
                .thenReturn(new Fields(keyField));

        bolt.prepare(stormConfig, context, collector);

        logger.info("Sending tuple.");
        Values values = new Values(4);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);
        Thread.sleep(1000);

        ArgumentCaptor<Tuple> tupleCaptor = ArgumentCaptor.forClass(Tuple.class);
        ArgumentCaptor<List> outputCaptor = ArgumentCaptor.forClass(List.class);
        verify(collector, times(1)).emit(any(), tupleCaptor.capture(), outputCaptor.capture());

        assertEquals(tuple, tupleCaptor.getValue());
        assertEquals(Arrays.asList(42), outputCaptor.getValue());

    }

    @Test
    @SuppressWarnings("unchecked")
    public void CassandraLookupBolt_MultiValueTest() throws Exception {

        session.execute(QueryBuilder.insertInto(KEYSPACE, lookupTable)
                .values(
                        Arrays.asList(keyField, "i2", "i3", "i4", "i5", "i6"),
                        Arrays.asList(4, 8, 15, 16, 23, 42)
                )
        );
        session.execute(QueryBuilder.insertInto(KEYSPACE, lookupTable)
                .values(
                        Arrays.asList(keyField, "i2", "i3", "i4", "i5", "i6"),
                        Arrays.asList(100, 1000, 1111, 10000, 10111, 42)
                )
        );

        logger.info("Preparing bolt.");
        CassandraLookupBolt bolt = new CassandraLookupBolt()
                .setForwardFields(new Fields("i6"))
                .setLookupMapper(new DefaultLookupMapper(
                        KEYSPACE,
                        lookupTable,
                        new Fields("i6"),
                        new Fields(keyField, "i2", "i3", "i4", "i5")));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);

        when(context.getSharedExecutor())
                .thenReturn(executor);
        // Input fields
        when(context.getComponentOutputFields(any(), any()))
                .thenReturn(new Fields("i6"));
        // Output fields
        when(context.getThisOutputFieldsForStreams())
                .thenReturn(getOutputFieldsForStreams("i6", keyField, "i2", "i3", "i4", "i5"));

        bolt.prepare(stormConfig, context, collector);

        logger.info("Sending tuple.");
        Values values = new Values(42);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);
        Thread.sleep(1000);

        ArgumentCaptor<Tuple> tupleCaptor = ArgumentCaptor.forClass(Tuple.class);
        ArgumentCaptor<List> outputCaptor = ArgumentCaptor.forClass(List.class);
        verify(collector, times(2)).emit(any(), tupleCaptor.capture(), outputCaptor.capture());

        assertEquals(tuple, tupleCaptor.getValue());

        List<List> outputs = outputCaptor.getAllValues();
        assertTrue(outputs.stream().anyMatch(emit -> Arrays.asList(42, 4, 8, 15, 16, 23).equals(emit)));
        assertTrue(outputs.stream().anyMatch(emit -> Arrays.asList(42, 100, 1000, 1111, 10000, 10111).equals(emit)));

    }

    private Map<String,List<String>> getOutputFieldsForStreams(String... fields) {
        Map<String, List<String>> map = new HashMap<>();
        map.put(DEFAULT_STREAM_ID, Arrays.asList(fields));
        return map;
    }

}
