package com.quanta.data;
import com.quanta.blob.*;

import java.io.File;
import java.io.IOException;
import java.sql.Types;

public class Dictionary extends DataAdapter<String> {
    private FixedRegion list;
    private VariableRegion data;

    public Dictionary(int maxWidth) {
        super(-1, true);

        data = new VariableRegion();
        list = new FixedRegion(maxWidth);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public Region[] regions() {
        return new Region[] {list, data};
    }

    @Override
    public int getDataType() {
        return Types.VARCHAR;
    }

    @Override
    public int add(String value) throws IOException {
        int pos = data.addString(value);
        return list.addInt(pos);

        //System.out.println(value + "\t" + pos + "\t" + i + "\t" + list.getLong(i));
        //return i;
    }

    @Override
    public String get(int index) throws IOException {
        int pos = list.getInt(index);
        return data.getString(pos);
    }

    @Override
    public int hash(String value) {
        return value.hashCode();
    }

    @Override
    public int compare(String v1, String v2) {
        return v1.compareTo(v2);
    }

    @Override
    public String parse(Object o) {
        return String.valueOf(o);
    }

    public static void main(String[] args) throws IOException {
        new File("c:/temp/dict").delete();

        Dictionary d = new Dictionary(20);
        Blob b = new MemoryBlob("c:/temp/dict", d.data, d.list);

        for (int i = 0; i < 10; i++) {
            d.add("Label " + i);
        }

        for (int i = 0; i < 10; i++) {
            System.out.println(d.get(i));
        }

        b.close();
    }
}
