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
import com.hmsonline.storm.cassandra.mapper.DefaultLookupMapper;
import com.hmsonline.storm.cassandra.testtools.TestBase;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CassandraLookupFunctionTest extends TestBase {

    private static Logger logger = LoggerFactory.getLogger(CassandraLookupFunctionTest.class);

    @Mock
    TridentOperationContext context;

    @Mock
    TridentTuple inputTuple;

    @Mock
    TridentCollector collector;

    @Test
    public void cassandraLookupFunction_SingleTest() throws InterruptedException {

        String table = "lookupTable";

        createTable(KEYSPACE, table,
                column("answer", DataType.cint()),
                column("question", DataType.varchar()));

        session.execute(QueryBuilder
                .insertInto(KEYSPACE, table)
                .values(
                        Arrays.asList("question", "answer"),
                        Arrays.asList("What do you get if you multiply six by nine?", 42)
                ));

        CassandraLookupFunction lookupFunction = new CassandraLookupFunction()
                .setLookupMapper(new DefaultLookupMapper(KEYSPACE, table, new Fields("answer"), new Fields("question")));

        lookupFunction.prepare(stormConfig, context);

        when(inputTuple.getValueByField(eq("answer"))).thenReturn(42);
        ArgumentCaptor<List> emitCaptor = ArgumentCaptor.forClass(List.class);

        lookupFunction.execute(inputTuple, collector);

        verify(collector, times(1)).emit(emitCaptor.capture());

        List<Object> outputTuple = emitCaptor.getValue();
        assertEquals(1, outputTuple.size());
        assertEquals("What do you get if you multiply six by nine?", outputTuple.get(0));

    }

    @Test
    public void cassandraLookupFunction_MultiTest() throws InterruptedException {

        String table = "lookupTable";

        createTable(KEYSPACE, table,
                column("id", DataType.cint()),
                column("answer", DataType.cint()),
                column("question", DataType.varchar()),
                column("source", DataType.varchar()));

        createIndex(KEYSPACE, table, "answer");

        session.execute(QueryBuilder
                .insertInto(KEYSPACE, table)
                .values(
                        Arrays.asList("id", "question", "answer", "source"),
                        Arrays.asList(1, "What do you get if you multiply six by nine?", 42, "earth")
                ));

        session.execute(QueryBuilder
                .insertInto(KEYSPACE, table)
                .values(
                        Arrays.asList("id", "question", "answer", "source"),
                        Arrays.asList(2, "What is the answer to life, the universe and everything", 42, "earth")
                ));

        CassandraLookupFunction lookupFunction = new CassandraLookupFunction()
                .setLookupMapper(new DefaultLookupMapper(KEYSPACE, table, new Fields("answer"), new Fields("question", "source")));

        lookupFunction.prepare(stormConfig, context);

        when(inputTuple.getValueByField(eq("answer"))).thenReturn(42);
        ArgumentCaptor<List> emitCaptor = ArgumentCaptor.forClass(List.class);

        lookupFunction.execute(inputTuple, collector);

        verify(collector, times(2)).emit(emitCaptor.capture());

        List<List> emits = emitCaptor.getAllValues();
        assertTrue(emits.stream()
                .anyMatch(emit -> emit.size() == 2
                        && emit.get(0).equals("What do you get if you multiply six by nine?")
                        && emit.get(1).equals("earth")));
        assertTrue(emits.stream()
                .anyMatch(emit -> emit.size() == 2
                        && emit.get(0).equals("What is the answer to life, the universe and everything")
                        && emit.get(1).equals("earth")));

    }




}

