package com.quanta.column;

import com.quanta.util.ByteBitSet;
import com.quanta.util.JSONWriter;
import com.quanta.data.DataAdapter;

import java.io.IOException;

public class AllUniqueValuesIndexColumn<T> extends SortedColumn<T> {

    public AllUniqueValuesIndexColumn(String name, String file, DataAdapter<T> adapter) throws IOException {
        super(name, adapter, Integer.MAX_VALUE);

        init(file, sorted.regions()[0]);
    }

    @Override
    public int getColumnType() {
        return 1;
    }

    @Override
    public void add(T value) throws IOException {
        int loc = adapter.add(value);
        sort(value, loc, true);
    }

    @Override
    protected void forIndex(ByteBitSet set, int index) throws IOException {
        set.set(index);
    }

    @Override
    public void writeMeta(JSONWriter json) throws IOException {
        json.newObject();

        json.write("name", name);
        json.write("index",   "unique");
        json.write("is_fact", "false", false);
        json.write("data", adapter.isString ? "text" : "int");
        json.newArray("values");
        json.closeArray();

        json.closeObject();
    }
}
