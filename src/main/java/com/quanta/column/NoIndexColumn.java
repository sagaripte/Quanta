package com.quanta.column;

import com.quanta.util.JSONWriter;
import com.quanta.data.DataAdapter;
import com.quanta.util.ByteBitSet;

import java.io.IOException;
import java.util.List;

public class NoIndexColumn<T> extends Column<T> {


    public NoIndexColumn(String name, String file, DataAdapter<T> adapter) throws IOException {
        super(name, adapter);

        init(file);
    }
    @Override
    public int getColumnType() {
        return 0;
    }

    @Override
    public ByteBitSet eq(List<T> list) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ByteBitSet not(List<T> list) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ByteBitSet gt(T value) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ByteBitSet lt(T value) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ByteBitSet between(T low, T high) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void writeMeta(JSONWriter json) throws IOException {

        json.newObject();

        json.write("name",    name);
        json.write("index",   "no");
        json.write("is_fact", "false", false);
        json.write("data",    adapter.isString ? "text" : "int");

        json.closeObject();
    }

    public String[] eqLabels(List<T> list) throws IOException {
        throw new UnsupportedOperationException("");
    }
    public String[] notEqLabels(List<T> list) throws IOException {
        throw new UnsupportedOperationException("");
    }
    public String[] gtLabels(T item) throws IOException {
        throw new UnsupportedOperationException("");
    }
    public String[] ltLabels(T item) throws IOException {
        throw new UnsupportedOperationException("");
    }
    public String[] betweenLabels(T one, T two) throws IOException {
        throw new UnsupportedOperationException("");
    }
    public String[] getAllLabels() throws IOException {
        throw new UnsupportedOperationException("");
    }
}
