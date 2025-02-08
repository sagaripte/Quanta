package com.quanta.data;

import java.io.IOException;
import java.sql.Types;

public class DoubleAdapter extends FixedWidthDataAdapter<Double> {

    public DoubleAdapter() {
        super(8, false);
    }

    @Override
    public void insert(int index, Double value) throws IOException {
        data.insert(index, Double.doubleToRawLongBits(value));
    }

    @Override
    public int getDataType() {
        return Types.DOUBLE;
    }

    @Override
    public int add(Double value) throws IOException {
        return data.addLong(Double.doubleToRawLongBits(value));
    }

    @Override
    public Double get(int index) throws IOException {
        return Double.longBitsToDouble(data.getLong(index));
    }

    public double getDouble(int index) throws IOException {
        return Double.longBitsToDouble(data.getLong(index));
    }

    @Override
    public int hash(Double value) {
        return value.hashCode();
    }

    @Override
    public int compare(Double v1, Double v2) {
        return v1.compareTo(v2);
    }

    @Override
    public Double parse(Object o) {
        if (o instanceof Double) {
            return (Double) o;
        } else if (o instanceof String) {
            return Double.parseDouble((String) o);
        } else if (o == null) {
            throw new NullPointerException("");
        } else {
            throw new ClassCastException("Expected int, Actual: " + o.getClass());
        }
    }
}
