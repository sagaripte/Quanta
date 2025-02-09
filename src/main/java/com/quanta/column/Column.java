package com.quanta.column;

import com.quanta.util.JSONWriter;
import com.quanta.blob.Blob;
import com.quanta.blob.MemoryBlob;
import com.quanta.blob.Region;
import com.quanta.data.DataAdapter;
import com.quanta.util.ByteBitSet;
import com.quanta.util.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * @param <T>
 */
public abstract class Column<T> implements Closeable {

    public final String name;
    protected Blob blob;
    protected DataAdapter<T> values;

    public Column(String name, DataAdapter<T> dataAdapter) {
        this.name   = name;
        this.values = dataAdapter;
    }

    protected void init(String file, Region...regions) throws IOException {
        if (regions == null)
            regions = new Region[0];

        Region[] va = values.regions();
        Region[] rp = new Region[va.length + regions.length];

        System.arraycopy(va, 0, rp, 0, va.length);

        for (int i = 0, j = va.length; i < regions.length; i++, j++)
            rp[j] = regions[i];

        blob = new MemoryBlob(file, rp);
    }

    private static final Pattern PRINT = Pattern.compile("\\P{Print}");

    public void add(T value) throws IOException {
        if (value instanceof String s) {
            s = s.trim();
            s = PRINT.matcher(s).replaceAll("");
            values.add((T)s);
        } else {
            values.add(value);
        }
    }

    public T get(int index) throws IOException {
        return values.get(index);
    }

    public String toString(int index) throws IOException {
        return values.toString(get(index));
    }

    public int size() {
        return values.size();
    }


    public abstract int getColumnType();

    public abstract ByteBitSet eq(List<T> list) throws IOException;
    public abstract ByteBitSet not(List<T> list) throws IOException;

    public abstract ByteBitSet gt(T value) throws IOException;
    public abstract ByteBitSet lt(T value) throws IOException;
    public abstract ByteBitSet between(T low, T high) throws IOException;

    public abstract void writeMeta(JSONWriter json) throws IOException;

    public void write(JSONWriter json, int index) throws IOException {
        json.writeValue(values.toString(get(index)), values.isString);
    }

    @Override
    public void close() throws IOException {
        blob.close();
    }

    public final ByteBitSet filter(String optr, Object value) throws IOException {
        if (Utils.isEmpty(optr))
            optr = "=";

        optr = optr.toLowerCase();

        boolean isArray = Utils.isArray(value);

        List<T> list = new ArrayList<>();

        if (isArray) {
            Object[] os = (Object[])value;
            for (Object o : os) {
                list.add(values.parse(o));
            }
        } else {
            list.add(values.parse(value));
        }

        if (optr.equals("=") || optr.equals("eq")) {
            return eq(list);
        } else if (optr.equals("!=") || optr.startsWith("not") || optr.equals("<>")) {
            return not(list);
        } else if (optr.equals(">") || optr.startsWith("gt")) {
            return gt(list.get(0));
        } else if (optr.equals("<") || optr.startsWith("lt")) {
            return lt(list.get(0));
        } else if (optr.equals("b") || optr.equals("r") || optr.equals("btwn") || optr.equals("between") || optr.equals("range")) {
            return between(list.get(0), list.get(1));
        } else {
            throw new IllegalArgumentException("Unknown operator: " + optr);
        }
    }

    public String[] getLabels(String optr, Object value) throws IOException {
        if (Utils.isEmpty(optr)) optr = "=";
        optr = optr.toLowerCase();

        boolean isArray = Utils.isArray(value);

        List<T> list = new ArrayList<>();

        if (isArray) {
            Object[] os = (Object[])value;
            for (Object o : os) {
                list.add(values.parse(o));
            }
        } else {
            list.add(values.parse(value));
        }

        if (optr.equals("=") || optr.equals("eq")) {
            return eqLabels(list);
        } else if (optr.equals("!=") || optr.startsWith("not") || optr.equals("<>")) {
            return notEqLabels(list);
        } else if (optr.equals(">") || optr.startsWith("gt")) {
            return gtLabels(list.get(0));
        } else if (optr.equals("<") || optr.startsWith("lt")) {
            return ltLabels(list.get(0));
        } else if (optr.equals("b") || optr.equals("r") || optr.equals("btwn") || optr.equals("between") || optr.equals("range")) {
            return betweenLabels(list.get(0), list.get(1));
        } else {
            throw new IllegalArgumentException("Unknown operator: " + optr);
        }
    }

    public abstract String[] getAllLabels() throws IOException;
    public abstract String[] eqLabels(List<T> list) throws IOException;
    public abstract String[] notEqLabels(List<T> list) throws IOException;
    public abstract String[] gtLabels(T item) throws IOException;
    public abstract String[] ltLabels(T item) throws IOException;
    public abstract String[] betweenLabels(T one, T two) throws IOException;

}
