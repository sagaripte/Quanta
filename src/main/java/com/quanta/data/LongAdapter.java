package com.quanta.data;

import java.io.IOException;
import java.sql.Types;

public class LongAdapter extends FixedWidthDataAdapter<Long> {
    public LongAdapter() {
        super(Long.BYTES, false);
    }

    @Override
    public int getDataType() {
        return Types.NUMERIC;
    }

    @Override
    public void insert(int index, Long value) throws IOException {
        data.insert(index, value);
    }

    @Override
    public int add(Long value) throws IOException {
        return data.addLong(value);
    }

    @Override
    public Long get(int index) throws IOException {
        return data.getLong(index);
    }

    @Override
    public int hash(Long value) {
        return value.hashCode();
    }

    @Override
    public int compare(Long v1, Long v2) {
        return v1.compareTo(v2);
    }

    @Override
    public Long parse(Object o) {
        if (o instanceof Long) {
            return (Long) o;
        } else if (o instanceof String) {
            return Long.parseLong((String) o);
        } else if (o == null) {
            throw new NullPointerException("");
        } else {
            throw new ClassCastException("Expected int, Actual: " + o.getClass());
        }
    }
}
