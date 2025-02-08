package com.quanta.data;

import java.io.IOException;
import java.sql.Types;

public class BooleanAdapter extends FixedWidthDataAdapter<Boolean> {
    public BooleanAdapter() {
        super(1, false);
    }

    @Override
    public int getDataType() {
        return Types.BOOLEAN;
    }

    @Override
    public int add(Boolean value) throws IOException {
        int s = data.size();
        data.addBoolean(value);
        return s;
    }

    @Override
    public void insert(int index, Boolean value) throws IOException {
        data.insert(index, value);
    }

    @Override
    public Boolean get(int index) throws IOException {
        return data.getBoolean(index);
    }

    @Override
    public int hash(Boolean value) {
        return value.hashCode();
    }

    @Override
    public int compare(Boolean v1, Boolean v2) {
        return v1.compareTo(v2);
    }

    @Override
    public Boolean parse(Object o) {
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else if (o instanceof String) {
            return Boolean.parseBoolean((String) o);
        } else if (o == null) {
            throw new NullPointerException("");
        } else {
            throw new ClassCastException("Expected int, Actual: " + o.getClass());
        }
    }
}
