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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.storm.cassandra.mapper.DefaultUpsertMapper;
import com.hmsonline.storm.cassandra.testtools.TestBase;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CassandraBoltTest extends TestBase {
    private static Logger logger = LoggerFactory.getLogger(CassandraBoltTest.class);

    private ExecutorService executor = Executors.newCachedThreadPool();

    @Mock
    TopologyContext context;

    @Test
    public void CassandraBolt_SingleValueTest() throws Exception {

        String outputTable = "bolt_output";
        String outputField = "value";
        Fields fields = new Fields(outputField);

        when(context.getComponentOutputFields(any(), any()))
                .thenReturn(new Fields(outputField));
        when(context.getSharedExecutor())
                .thenReturn(executor);

        logger.info("Creating cassandra table.");
        createTable(KEYSPACE, outputTable, column(outputField, DataType.cint()));

        logger.info("Preparing bolt.");
        CassandraBolt bolt = new CassandraBolt()
                .setTupleMapper(new DefaultUpsertMapper(KEYSPACE, outputTable, fields));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);

        bolt.prepare(stormConfig, context, null);

        logger.info("Sending tuple.");
        Values values = new Values(42);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        Select select = QueryBuilder
                .select()
                .all()
                .from(KEYSPACE, outputTable);

        ResultSet result;
        do {
            Thread.sleep(500);
            logger.info("Querying for a value...");
            result = session.execute(select);
        }
        while (!result.iterator().hasNext());

        int answer = result.one().getInt(outputField);
        logger.info("The answer is clearly {}, now what was the question?", answer);
        assertEquals(42, answer);

    }

    @Test
    public void CassandraBolt_MultiValueTest() throws Exception {

        String outputTable = "bolt_output";
        String[] outputFields = { "n1","n2","n3","n4","n5","n6" };
        Fields fields = new Fields(outputFields);

        when(context.getComponentOutputFields(any(), any()))
                .thenReturn(new Fields(outputFields));
        when(context.getSharedExecutor())
                .thenReturn(executor);

        logger.info("Creating cassandra table.");
        createTable(KEYSPACE, outputTable,
                column(outputFields[0], DataType.cint()),
                column(outputFields[1], DataType.cint()),
                column(outputFields[2], DataType.cint()),
                column(outputFields[3], DataType.cint()),
                column(outputFields[4], DataType.cint()),
                column(outputFields[5], DataType.cint())
        );

        logger.info("Preparing bolt.");
        CassandraBolt bolt = new CassandraBolt()
                .setTupleMapper(new DefaultUpsertMapper(KEYSPACE, outputTable, fields));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("TEST_BOLT", bolt);


        bolt.prepare(stormConfig, context, null);

        logger.info("Sending tuple.");
        Values values = new Values(4, 8, 15, 16, 23, 42);
        Tuple tuple = new TupleImpl(context, values, 5, "test");
        bolt.execute(tuple);

        Select select = QueryBuilder
                .select()
                .all()
                .from(KEYSPACE, outputTable);

        ResultSet result;
        do {
            Thread.sleep(500);
            logger.info("Querying for a value...");
            result = session.execute(select);
        }
        while (!result.iterator().hasNext());

        Row answer = result.one();
        assertEquals(4, answer.getInt(0));
        assertEquals(8, answer.getInt(1));
        assertEquals(15, answer.getInt(2));
        assertEquals(16, answer.getInt(3));
        assertEquals(23, answer.getInt(4));
        assertEquals(42, answer.getInt(5));

    }

}
