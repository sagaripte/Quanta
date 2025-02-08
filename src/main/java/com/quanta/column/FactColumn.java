package com.quanta.column;

import com.quanta.data.DataAdapter;
import com.quanta.util.ByteBitSet;
import com.quanta.util.JSONWriter;
import com.quanta.data.DoubleAdapter;

import java.io.IOException;
import java.util.List;

public class FactColumn<T> extends Column<T> {

    public FactColumn(String name, String file, DataAdapter<T> adapter) throws IOException {
        super(name, adapter);

        init(file);
    }
    @Override
    public int getColumnType() {
        return 5;
    }

    public double getDouble(int index) throws IOException {
        return (Double)super.adapter.get(index);
    }
    public double getInt(int index) throws IOException {
        return (Integer)super.adapter.get(index);
    }

    @Override
    public ByteBitSet eq(List<T> values) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public ByteBitSet not(List<T> values) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public ByteBitSet gt(T value) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public ByteBitSet lt(T value) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public ByteBitSet between(T low, T high) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public void writeMeta(JSONWriter json) throws IOException {
        json.newObject();

        json.write("name",    name);
        json.write("index",   "no");
        json.write("is_fact", "true", false);
        json.write("data",    "int");

        json.closeObject();
    }

    @Override
    public void close() throws IOException {
        blob.close();
    }

    @Override
    public String[] getAllLabels() throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public String[] eqLabels(List<T> list) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public String[] notEqLabels(List<T> list) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public String[] gtLabels(T item) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public String[] ltLabels(T item) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }

    @Override
    public String[] betweenLabels(T one, T two) throws IOException {
        throw new UnsupportedOperationException("This operation is not supported on FactColumn");
    }
}
