package com.hmsonline.storm.cassandra.util;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Utility class for handling ordered key/value data with an immutable key set.
 * This is useful because the storm api often uses index based mappings of keys and values.
 */
public class DataTuple implements ITuple, Serializable {

    private static final long serialVersionUID = -8275529219132465229L;

    private final List<String> keys;
    private List<Object> values;

    public DataTuple(Iterable<String> keys) {
        this.keys = new ArrayList<>();
        keys.forEach(this.keys::add);
        this.values = Arrays.asList(new Object[this.keys.size()]);
    }

    public DataTuple(Iterable<String> keys, Iterable<?> values) {
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        keys.forEach(this.keys::add);
        values.forEach(this.values::add);
    }

    protected DataTuple(List<String> keys) {
        this.keys = keys;
        this.values = Arrays.asList(new Object[this.keys.size()]);
    }

    protected DataTuple() {
        throw new UnsupportedOperationException();
    }

    public DataTuple put(String key, Object value) {
        int index = keys.indexOf(key);
        if (index >= 0) {
            values.set(index, value);
        }
        else {
            throw new IllegalArgumentException("Field " + key + " does not exist.");
        }
        return this;
    }

    public String[] keys() {
        return keys.toArray(new String[keys.size()]);
    }

    public Object[] values() {
        return values.toArray();
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), values.get(i));
        }
        return map;
    }

    public static DataTuple fromMap(Map<String, Object> map) {
        DataTuple dataMap = new DataTuple(map.keySet());
        map.forEach(dataMap::put);
        return dataMap;
    }

    public DataTuple setValues(List<Object> values) {
        this.values = new ArrayList<>(values);
        return this;
    }

    @Override
    public int size() {
        return keys.size();
    }

    @Override
    public boolean contains(String field) {
        return keys.contains(field);
    }

    @Override
    public Fields getFields() {
        return new Fields(keys);
    }

    @Override
    public int fieldIndex(String field) {
        return keys.indexOf(field);
    }

    @Override
    public List<Object> select(Fields selector) {
        List<Object> values = new ArrayList<>();
        selector.forEach(values::add);
        return values;
    }

    @Override
    public Object getValue(int i) {
        return values.get(i);
    }

    @Override
    public String getString(int i) {
        return (String) values.get(i);
    }

    @Override
    public Integer getInteger(int i) {
        return (Integer) values.get(i);
    }

    @Override
    public Long getLong(int i) {
        return (Long) values.get(i);
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean) values.get(i);
    }

    @Override
    public Short getShort(int i) {
        return (Short) values.get(i);
    }

    @Override
    public Byte getByte(int i) {
        return (Byte) values.get(i);
    }

    @Override
    public Double getDouble(int i) {
        return (Double) values.get(i);
    }

    @Override
    public Float getFloat(int i) {
        return (Float) values.get(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) values.get(i);
    }

    @Override
    public Object getValueByField(String field) {
        return values.get(keys.indexOf(field));
    }

    @Override
    public String getStringByField(String field) {
        return (String) getValueByField(field);
    }

    @Override
    public Integer getIntegerByField(String field) {
        return (Integer) getValueByField(field);
    }

    @Override
    public Long getLongByField(String field) {
        return (Long) getValueByField(field);
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return (Boolean) getValueByField(field);
    }

    @Override
    public Short getShortByField(String field) {
        return (Short) getValueByField(field);
    }

    @Override
    public Byte getByteByField(String field) {
        return (Byte) getValueByField(field);
    }

    @Override
    public Double getDoubleByField(String field) {
        return (Double) getValueByField(field);
    }

    @Override
    public Float getFloatByField(String field) {
        return (Float) getValueByField(field);
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return (byte[]) getValueByField(field);
    }

    @Override
    public List<Object> getValues() {
        return Collections.unmodifiableList(values);
    }

    public List<String> getKeys() {
        return Collections.unmodifiableList(keys);
    }

    public static Supplier<DataTuple> builder(Fields fields) {
        List<String> keys = Collections.unmodifiableList(fields.toList());
        return () -> new DataTuple(keys);
    }

    public static Supplier<DataTuple> builder(List<String> fields) {
        List<String> keys = Collections.unmodifiableList(fields);
        return () -> new DataTuple(keys);
    }

    public void forEach(BiConsumer<String, Object> handler) {
        for (int i = 0; i < keys.size(); i++) {
            handler.accept(keys.get(i), values.get(i));
        }
    }

}