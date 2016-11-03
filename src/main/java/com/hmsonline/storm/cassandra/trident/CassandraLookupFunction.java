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

package com.hmsonline.storm.cassandra.trident;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.storm.cassandra.Constants;
import com.hmsonline.storm.cassandra.client.JsonClusterConfigurator;
import com.hmsonline.storm.cassandra.client.SessionFactory;
import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;
import com.hmsonline.storm.cassandra.exceptions.MappedExceptionHandler;
import com.hmsonline.storm.cassandra.util.DataTuple;
import com.hmsonline.storm.cassandra.mapper.LookupMapper;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.hmsonline.storm.cassandra.util.CassandraUtils.getStatementCacheKey;

/**
 * A bolt implementation that loads a row from cassandra using a tuple tupleToCassandraMapper, and emits fields based on output fields.
 * <p>
 * If no tuple tupleToCassandraMapper is specified, the input tuple fields will be used.
 * 
 * @author tgoetz
 */

@SuppressWarnings("serial")
public class CassandraLookupFunction extends BaseFunction {

    public static final String DEFAULT_CASSANDRA_CONFIG_KEY = Constants.DEFAULT_CASSANDRA_CONFIG_KEY;
    private static final Logger logger = LoggerFactory.getLogger(CassandraLookupFunction.class);

    private ExceptionHandler exceptionHandler;
    private LookupMapper lookupMapper;
    private Map<String, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();
    private Session session;
    private String cassandraConfigKey = DEFAULT_CASSANDRA_CONFIG_KEY;

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        session = SessionFactory.getSingletonSession(
                cassandraConfigKey,
                () -> new JsonClusterConfigurator(cassandraConfigKey, (Map<String, Object>) conf.get(cassandraConfigKey)));
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void execute(TridentTuple input, TridentCollector collector) {
        try {
            String keyspace = lookupMapper.mapToKeyspace(input);
            String table = lookupMapper.mapToTable(input);
            DataTuple rowKey = lookupMapper.mapToRowKey(input);
            Fields columns = lookupMapper.getColumns();
            Fields outputFields = lookupMapper.getOutputFields();

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

            session.execute(statement)
                    .forEach(row -> {
                        Values values = lookupMapper.mapToValues(row);
                        collector.emit(values);
                    });

        } catch (Exception e) {
            getExceptionHandler().onException(e, collector::reportError, logger);
        }
    }

    public LookupMapper getLookupMapper() {
        return lookupMapper;
    }

    public CassandraLookupFunction setLookupMapper(LookupMapper lookupMapper) {
        this.lookupMapper = lookupMapper;
        return this;
    }

    protected PreparedStatement prepareStatement(
            String name,
            Supplier<RegularStatement> statementSupplier) {

        return preparedStatements.computeIfAbsent(name, key -> {
            RegularStatement statement = statementSupplier.get();
            return session.prepare(statement);
        });

    }

    public ExceptionHandler getExceptionHandler() {
        if (exceptionHandler == null) {
            exceptionHandler = new MappedExceptionHandler();
        }
        return exceptionHandler;
    }

    public CassandraLookupFunction setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public String getCassandraConfigKey() {
        return cassandraConfigKey;
    }

    public CassandraLookupFunction setCassandraConfigKey(String cassandraConfigKey) {
        this.cassandraConfigKey = cassandraConfigKey;
        return this;
    }

}
