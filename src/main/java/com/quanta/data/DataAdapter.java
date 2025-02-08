package com.quanta.data;

import com.quanta.blob.Region;

import java.io.IOException;

public abstract class DataAdapter<T> {

    public String name;
    public final int width;
    public final boolean isString;

    public DataAdapter(int width, boolean isString) {
        this.width = width;
        this.isString = isString;
    }

    public abstract int size();

    public abstract Region[] regions();

    public abstract int getDataType();

   // public abstract void write() throws IOException;

    public abstract int add(T value) throws IOException;

    public abstract T get(int index) throws IOException;

    //public abstract byte[] getBytes(int index) throws IOException;

    //public abstract byte[] toBytes(T value);

    public abstract int hash(T value);

    public abstract int compare(T v1, T v2);

    public String toString(T v) {
        return v == null ? "" : v.toString();
    }

    public abstract T parse(Object o);
}