package com.hmsonline.storm.cassandra.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;

/**
 * Helper class which keeps keys and values in a predefined order.
 * This is useful for asynchronously collecting values and matching them into the right value slot for
 * state operations.
 * @param <T> The value
 */
public class ValueMatcher<T> {

    private LinkedHashMap<List, T> valueMap;

    public  ValueMatcher(List<List<Object>> keys) {
        valueMap = new LinkedHashMap<>();
        keys.forEach(key -> valueMap.put(new KeyMatcher(key), null));
    }

    public void put(List<Object> key, T value) {
        if (!valueMap.containsKey(key)) {
            throw new IllegalArgumentException("Key was not present in value matcher: " + key.toString());
        }
        valueMap.put(key, value);
    }

    public List<T> getValues() {
        return new ArrayList<>(valueMap.values());
    }

    /**
     * A class that matches keys (as List&lt;Object&gt;) against a row (as List&lt;Object&gt;),
     * assuming the row contains the keys in the same order at the beginning of the row.
     */
    private class KeyMatcher extends ArrayList<Object> {

        private static final long serialVersionUID = -933558150147476412L;

        public KeyMatcher(List<Object> keys) {
            super(keys);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof List) {
                ListIterator<Object> keyValues = listIterator();
                ListIterator<?> otherValues = ((List<?>) obj).listIterator();
                while (keyValues.hasNext() && otherValues.hasNext()) {
                    Object o1 = keyValues.next();
                    Object o2 = otherValues.next();
                    if (!(o1==null ? o2==null : o1.equals(o2)))
                        return false;
                }
                return !(keyValues.hasNext());
            }
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

    }

}

