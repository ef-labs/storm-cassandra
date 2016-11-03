/* Licensed to the Apache Software Foundation (ASF) under one
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
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.storm.cassandra.util.DataTuple;
import com.hmsonline.storm.cassandra.mapper.LookupMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.hmsonline.storm.cassandra.util.CassandraUtils.getStatementCacheKey;

/**
 * A bolt implementation that loads a row from cassandra using a tuple tupleToCassandraMapper, and emits fields based on output fields.
 * <p>
 * If no tuple tupleToCassandraMapper is specified, the input tuple fields will be used.
 * 
 * @author tgoetz
 */

@SuppressWarnings("serial")
public class CassandraLookupBolt extends AbstractCassandraBolt<CassandraLookupBolt> {
    private static final Logger logger = LoggerFactory.getLogger(CassandraLookupBolt.class);

    private LookupMapper lookupMapper;
    private Fields forwardFields;
    private Map<String, List<String>> outputFieldsForStreams;
    private Fields columns;
    private Fields outputFields;
    private ArrayList mapFields;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        outputFieldsForStreams = context.getThisOutputFieldsForStreams();

        if (forwardFields == null) {
            forwardFields = new Fields();
        }

        columns = lookupMapper.getColumns();
        outputFields = lookupMapper.getOutputFields();
        mapFields = new ArrayList<>(outputFields.toList());
        mapFields.removeAll(forwardFields.toList());

    }

    @Override
    @SuppressWarnings("Duplicates")
    public void execute(Tuple input) {
        try {
            String keyspace = lookupMapper.mapToKeyspace(input);
            String table = lookupMapper.mapToTable(input);
            DataTuple rowKey = lookupMapper.mapToRowKey(input);

            String cacheKey = getStatementCacheKey(this.getClass().getSimpleName(), keyspace, table, rowKey.getKeys(), columns);

            BoundStatement statement = prepareStatement(
                    cacheKey,
                    () -> {
                        Select.Where select = QueryBuilder
                                .select(columns.toList().toArray(new String[columns.size()]))
                                .from(keyspace, table)
                                .where();

                        rowKey.getKeys()
                                .forEach(k -> select.and(QueryBuilder.eq(k, QueryBuilder.bindMarker())));

                        return select;
                    })
                    .bind(rowKey.values());

            getExecutor().executeAsync(statement, rows ->
                    rows.forEach(row -> {
                        outputFieldsForStreams
                                .forEach((stream, meta) -> {
                                    // Retrieve and loaded fields
                                    Values loadedValues = lookupMapper.mapToValues(row);
                                    // Build output from forwarded and loaded values
                                    Values output = new Values();
                                    forwardFields.forEach(f -> output.add(input.getValueByField(f)));
                                    output.addAll(loadedValues);
                                    collector.emit(stream, input, output);
                                });
                    }));

        } catch (Exception e) {
            getExceptionHandler().onException(e, collector, logger);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> allFields = new ArrayList<>();
        allFields.addAll(forwardFields.toList());
        allFields.addAll(lookupMapper.getOutputFields().toList());
        outputFieldsForStreams
                .forEach((stream, output) -> declarer.declareStream(stream, new Fields(allFields)));
    }

    public LookupMapper getLookupMapper() {
        return lookupMapper;
    }

    public CassandraLookupBolt setLookupMapper(LookupMapper lookupMapper) {
        this.lookupMapper = lookupMapper;
        return this;
    }

    public Fields getForwardFields() {
        return forwardFields;
    }

    public CassandraLookupBolt setForwardFields(Fields forwardFields) {
        this.forwardFields = forwardFields;
        return this;
    }

    private class StreamMetaData {
        public List<String> outputFields;
    }

}
