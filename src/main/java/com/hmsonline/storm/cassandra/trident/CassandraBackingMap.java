package com.hmsonline.storm.cassandra.trident;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.storm.cassandra.exceptions.ExceptionHandler;
import com.hmsonline.storm.cassandra.util.CassandraExecutor;
import com.hmsonline.storm.cassandra.util.ValueMatcher;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.map.IBackingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CassandraBackingMap<T> implements IBackingMap<T> {

    private static final Logger logger = LoggerFactory.getLogger(CassandraBackingMap.class);

    private final CassandraExecutor executor;
    private final String stateField;
    private final List<String> keyFields;
    private final Serializer<T> serializer;
    private final PreparedStatement preparedSelect;
    private final PreparedStatement preparedInsert;
    private final ExceptionHandler exceptionHandler;

    public CassandraBackingMap(CassandraExecutor executor, String keyspace, String table, String[] keyFields, String stateField, Serializer<T> serializer, ExceptionHandler exceptionHandler) {

        this.executor = executor;
        this.stateField = stateField;
        this.keyFields = Arrays.asList(keyFields);
        this.serializer = serializer;
        this.exceptionHandler = exceptionHandler;

        Select.Where selectQuery = QueryBuilder
                .select(stateField)
                .from(keyspace, table)
                .where();
        this.keyFields.forEach(k -> selectQuery.and(QueryBuilder.eq(k, QueryBuilder.bindMarker())));
        preparedSelect = executor.prepare(selectQuery);

        logger.debug("Prepared select statement: {}", selectQuery);

        List<String> keyAndDataNamesList = new ArrayList<>(this.keyFields);
        keyAndDataNamesList.add(stateField);

        Insert insertStatement = QueryBuilder
                .insertInto(keyspace, table)
                .values(keyAndDataNamesList, Collections.nCopies(keyAndDataNamesList.size(), QueryBuilder.bindMarker()));
        preparedInsert = executor.prepare(insertStatement);

        logger.debug("Prepared insert statement: {}", insertStatement);

    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        logger.debug("Loading {} values", keys.size());
        try {
            ValueMatcher<T> values = new ValueMatcher<>(keys);
            executor.executeAndWait(preparedSelect, keys, (key, rows) -> {
                Row row = rows.one();
                if (row != null) {
                    T value = serializer.deserialize(row.getBytes(stateField).array());
                    values.put(key, value);
                }
            });

            return values.getValues();
        }
        catch (Exception e) {
            exceptionHandler.onException(e, error -> logger.error("Failed to get values.", error), logger);
            throw e;
        }
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        logger.debug("Writing {} values", keys.size());
        try {
            List<List<Object>> bindings = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                List<Object> rowValues = new ArrayList<>(keys.get(i));
                rowValues.add(ByteBuffer.wrap(serializer.serialize(values.get(i))));
                bindings.add(rowValues);
            }
            executor.executeAndWait(preparedInsert, bindings, null);
        }
        catch (Exception e) {
            exceptionHandler.onException(e, error -> logger.error("Failed to get values.", error), logger);
            throw e;
        }
    }

}
