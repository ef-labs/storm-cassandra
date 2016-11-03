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

import com.datastax.driver.core.*;
import com.hmsonline.storm.cassandra.Constants;
import com.hmsonline.storm.cassandra.client.JsonClusterConfigurator;
import com.hmsonline.storm.cassandra.client.SessionFactory;
import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;
import com.hmsonline.storm.cassandra.exceptions.MappedExceptionHandler;
import com.hmsonline.storm.cassandra.util.CassandraExecutor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@SuppressWarnings("serial")
public abstract class AbstractCassandraBolt<T extends AbstractCassandraBolt> extends BaseRichBolt implements Serializable {

    public static final String DEFAULT_CASSANDRA_CONFIG_KEY = Constants.DEFAULT_CASSANDRA_CONFIG_KEY;
    private static final Logger logger = LoggerFactory.getLogger(AbstractCassandraBolt.class);

    private String cassandraConfigKey = DEFAULT_CASSANDRA_CONFIG_KEY;
    private ExceptionHandler exceptionHandler;

    private Map<String, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();
    private CassandraExecutor executor;
    private Session session;

    protected OutputCollector collector;

    public AbstractCassandraBolt() {
        logger.debug("Creating Cassandra Bolt (" + this + ")");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        session = SessionFactory.getSingletonSession(
                cassandraConfigKey,
                () -> new JsonClusterConfigurator(cassandraConfigKey, (Map<String, Object>) stormConf.get(cassandraConfigKey)));
        executor = new CassandraExecutor(session, context.getSharedExecutor(), 0);
        this.collector = collector;
    }

    @Override
    public void cleanup() {
        if (executor != null) {
            this.session.close();
        }
    }

    protected void ack(Tuple input) throws Exception {
        this.collector.ack(input);
    }

    protected PreparedStatement prepareStatement(
            String name,
            Supplier<RegularStatement> statementSupplier) {

        return preparedStatements.computeIfAbsent(name, key -> {
            RegularStatement statement = statementSupplier.get();
            return session.prepare(statement);
        });

    }

    public String getCassandraConfigKey() {
        return cassandraConfigKey;
    }

    @SuppressWarnings("unchecked")
    public T setCassandraConfigKey(String clientConfigKey) {
        this.cassandraConfigKey = clientConfigKey;
        return (T) this;
    }

    public ExceptionHandler getExceptionHandler() {
        if (exceptionHandler == null) {
            exceptionHandler = new MappedExceptionHandler();
        }
        return exceptionHandler;
    }

    public AbstractCassandraBolt setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public CassandraExecutor getExecutor() {
        return executor;
    }
}
