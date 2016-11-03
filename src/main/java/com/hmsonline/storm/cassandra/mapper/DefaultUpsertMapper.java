package com.hmsonline.storm.cassandra.mapper;

import com.hmsonline.storm.cassandra.exceptions.UnrecoverableException;
import com.hmsonline.storm.cassandra.util.DataTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

public class DefaultUpsertMapper implements UpsertMapper {

    private static final long serialVersionUID = -538083869428902047L;

    private final String keyspace;
    private final String table;
    private final Fields fields;

    public DefaultUpsertMapper(String keyspace, String table, Fields fields) {
        this.keyspace = keyspace;
        this.table = table;
        this.fields = fields;
    }

    @Override
    public DataTuple mapToColumns(ITuple tuple) {
        try {
            DataTuple map = new DataTuple(fields);
            fields.forEach(f -> map.put(f, tuple.getValueByField(f)));
            return map;
        }
        catch (Exception e) {
            throw new UnrecoverableException("Failed to map tuple to cassandra columns.", e);
        }
    }

    @Override
    public String mapToTable(ITuple tuple) {
        return this.table;
    }

    @Override
    public String mapToKeyspace(ITuple tuple) {
        return this.keyspace;
    }

    public Fields getFields() {
        return fields;
    }

    public String getKeyspace() {
        return this.keyspace;
    }

    public String getTable() {
        return this.table;
    }

}
