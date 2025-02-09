package com.quanta.data;

import com.quanta.blob.FixedRegion;
import com.quanta.blob.Region;

import java.io.IOException;

public abstract class FixedWidthDataAdapter<T> extends DataAdapter<T> {

    protected FixedRegion data;

    public FixedWidthDataAdapter(int width, boolean isString) {
        super(width, isString);

        this.data = new FixedRegion(width);
    }

    public abstract void insert(int index, T value) throws IOException;

    public int getInt(int index) throws IOException {
        throw new UnsupportedOperationException("");
    }

    public byte[] toBytes(T value) {
        throw new UnsupportedOperationException("");
    }

    public boolean match(byte[] rawData, byte[] toCompare, int index) {
        int offset = index * width;
        boolean match = true;
        for (int j = 0; j < width; j++) {
            if (rawData[offset + j] != toCompare[j]) {
                match = false;
                break;
            }
        }

        return match;
    }

    public byte[] getRawBytes(int fromRow, int count) throws IOException {
        return data.getRawBytes(fromRow, count);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Region[] regions() {
        return new Region[] {data};
    }


    public void print() throws IOException {
        for (int i = 0; i < data.size(); i++) {
            System.out.println(i + " \t" + get(i));
        }
    }
}
