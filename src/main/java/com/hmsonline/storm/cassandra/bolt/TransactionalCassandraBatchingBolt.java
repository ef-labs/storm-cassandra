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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.hmsonline.storm.cassandra.util.CassandraExecutor;
import com.hmsonline.storm.cassandra.util.DataTuple;
import com.hmsonline.storm.cassandra.mapper.UpsertMapper;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.IBatchBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static com.hmsonline.storm.cassandra.util.CassandraUtils.getStatementCacheKey;

@SuppressWarnings({ "serial", "rawtypes" })
public class TransactionalCassandraBatchingBolt extends AbstractCassandraBolt implements IBatchBolt,
        ICommitter {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalCassandraBatchingBolt.class);

    private Object transactionId = null;
    private LinkedBlockingQueue<Tuple> queue;
    private AckStrategy ackStrategy;
    private UpsertMapper tupleMapper;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        this.queue = new LinkedBlockingQueue<>();
        this.transactionId = id;
        logger.debug("Preparing cassandra batch [{}].", transactionId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // By default we don't emit anything.
    }

    @Override
    public void execute(Tuple input) {
        try {
            if (this.ackStrategy == AckStrategy.ACK_ON_RECEIVE) {
                this.ack(input);
            }
            queue.add(input);
        } catch (Exception e) {
            getExceptionHandler().onException(e, collector, logger);
        }
    }

    @Override
    public void finishBatch() {
        List<Tuple> batch = new ArrayList<>();
        int size = queue.drainTo(batch);
        logger.debug("Finishing batch for [{}], writing [{}] tuples.", transactionId, size);
        try {
            this.insert(batch);
            if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
                ack(batch);
            }
        } catch (Exception e) {
            getExceptionHandler().onException(e, collector, logger);
        }

    }

    protected void insert(List<Tuple> inputs) throws Exception {
        UpsertMapper tupleMapper = getTupleMapper();
        List<BoundStatement> statements = inputs.stream()
                .map(input -> {
                    String keyspace = tupleMapper.mapToKeyspace(input);
                    String table = tupleMapper.mapToTable(input);
                    DataTuple data = tupleMapper.mapToColumns(input);
                    List<String> allKeys = data.getKeys();
                    String cacheKey = getStatementCacheKey(this.getClass().getSimpleName(), keyspace, table, allKeys);
                    return prepareStatement(
                            cacheKey,
                            () -> QueryBuilder
                                    .insertInto(keyspace, table)
                                    .values(allKeys, Collections.nCopies(allKeys.size(), QueryBuilder.bindMarker())))
                            .bind(data.values());
                })
                .collect(Collectors.toList());

        getExecutor().executeAndWait(inputs, statements, null);

    }
    private void ack(List<Tuple> batch) throws Exception {
        for (Tuple tuple : batch) {
            ack(tuple);
        }
    }

    public AckStrategy getAckStrategy() {
        return this.ackStrategy;
    }

    public TransactionalCassandraBatchingBolt setAckStrategy(AckStrategy ackStrategy) {
        this.ackStrategy = ackStrategy;
        return this;
    }

    public UpsertMapper getTupleMapper() {
        return tupleMapper;
    }

    public TransactionalCassandraBatchingBolt setTupleMapper(UpsertMapper tupleMapper) {
        this.tupleMapper = tupleMapper;
        return this;
    }

}
