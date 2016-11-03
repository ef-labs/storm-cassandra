package com.hmsonline.storm.cassandra.mapper;

import com.datastax.driver.core.Row;
import com.hmsonline.storm.cassandra.util.DataTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.function.Supplier;

public class DefaultLookupMapper implements LookupMapper {

    private static final long serialVersionUID = 1797820191881195465L;

    private final String keyspace;
    private final String table;
    private final Fields keyFields;
    private final Fields columnFields;
    private Supplier<DataTuple> rowKeySupplier;

    public DefaultLookupMapper(String keyspace, String table, Fields keyFields, Fields columnFields) {
        this.keyspace = keyspace;
        this.table = table;
        this.keyFields = keyFields;
        this.columnFields = columnFields;
    }

    @Override
    public String mapToKeyspace(ITuple tuple) {
        return keyspace;
    }

    @Override
    public String mapToTable(ITuple tuple) {
        return table;
    }

    @Override
    public DataTuple mapToRowKey(ITuple tuple) {
        if (rowKeySupplier == null) {
            rowKeySupplier = DataTuple.builder(keyFields);
        }
        DataTuple rowKey = rowKeySupplier.get();
        rowKey.getKeys().forEach(field -> rowKey.put(field, tuple.getValueByField(field)));
        return rowKey;
    }

    @Override
    public Values mapToValues(Row row) {
        Values values = new Values();
        columnFields.forEach(field -> values.add(row.getObject(field)));
        return values;
    }

    @Override
    public Fields getKeys() {
        return keyFields;
    }

    @Override
    public Fields getColumns() {
        return columnFields;
    }

    @Override
    public Fields getOutputFields() {
        return columnFields;
    }



}
