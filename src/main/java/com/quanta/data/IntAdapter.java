package com.quanta.data;

import java.io.IOException;
import java.sql.Types;

public class IntAdapter extends FixedWidthDataAdapter<Integer> {
    public IntAdapter() {
        super(Integer.BYTES, false);
    }

    @Override
    public int getDataType() {
        return Types.INTEGER;
    }

    @Override
    public void insert(int index, Integer value) throws IOException {
        data.insert(index, value);
    }

    @Override
    public int add(Integer value) throws IOException {
        return data.addInt(value);
    }

    @Override
    public Integer get(int index) throws IOException {
        return data.getInt(index);
    }

    @Override
    public int getInt(int index) throws IOException {
        return data.getInt(index);
    }

    @Override
    public int hash(Integer value) {
        return value;
    }

    @Override
    public int compare(Integer v1, Integer v2) {
        return v1.compareTo(v2);
    }

    @Override
    public Integer parse(Object o) {
        if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof String) {
            return Integer.parseInt((String) o);
        } else if (o == null) {
            throw new NullPointerException("");
        } else {
            throw new ClassCastException("Expected int, Actual: " + o.getClass());
        }
    }
}
