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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.hmsonline.storm.cassandra.exceptions.RecoverableException;
import com.hmsonline.storm.cassandra.util.CassandraExecutor;
import com.hmsonline.storm.cassandra.util.DataTuple;
import com.hmsonline.storm.cassandra.mapper.UpsertMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import static com.hmsonline.storm.cassandra.util.CassandraUtils.getStatementCacheKey;

@SuppressWarnings("serial")
public class CassandraBolt extends AbstractCassandraBolt<CassandraBolt> {

    private static final Logger logger = LoggerFactory.getLogger(CassandraBolt.class);

    private UpsertMapper tupleMapper;
    private AckStrategy ackStrategy = AckStrategy.ACK_IGNORE;

    @Override
    public void execute(Tuple input) {
        try {
            if (ackStrategy == AckStrategy.ACK_ON_RECEIVE) {
                ack(input);
            }
            insert(input, rows -> {
                if (ackStrategy == AckStrategy.ACK_ON_WRITE) {
                    try {
                        ack(input);
                    } catch (Exception e) {
                        getExceptionHandler().onException(e, collector, logger);
                    }
                }
            });

        } catch (Exception e) {
            getExceptionHandler().onException(e, collector, logger);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output
    }

    public AckStrategy getAckStrategy() {
        return ackStrategy;
    }

    public CassandraBolt setAckStrategy(AckStrategy ackStrategy) {
        this.ackStrategy = ackStrategy;
        return this;
    }

    protected void insert(Tuple input, Consumer<ResultSet> handler) throws Exception {
        String keyspace = tupleMapper.mapToKeyspace(input);
        String table = tupleMapper.mapToTable(input);
        DataTuple data = tupleMapper.mapToColumns(input);
        List<String> allKeys = data.getKeys();
        String cacheKey = getStatementCacheKey(this.getClass().getSimpleName(), keyspace, table, allKeys);
        PreparedStatement insert = prepareStatement(
                cacheKey,
                () -> QueryBuilder
                        .insertInto(keyspace, table)
                        .values(allKeys, Collections.nCopies(allKeys.size(), QueryBuilder.bindMarker()))
        );
        getExecutor().executeAsync(insert, data.getValues(), handler);
    }

    public UpsertMapper getTupleMapper() {
        return tupleMapper;
    }

    @SuppressWarnings("unchecked")
    public CassandraBolt setTupleMapper(UpsertMapper tupleMapper) {
        this.tupleMapper = tupleMapper;
        return this;
    }

}
