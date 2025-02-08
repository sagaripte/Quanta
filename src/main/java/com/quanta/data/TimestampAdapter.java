package com.quanta.data;

import java.io.IOException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimestampAdapter extends FixedWidthDataAdapter<Timestamp> {
    private SimpleDateFormat sdf;

    public TimestampAdapter(String format) {
        super(Long.BYTES, true);
        sdf = new SimpleDateFormat(format);
    }

    @Override
    public int getDataType() {
        return Types.TIMESTAMP;
    }

    @Override
    public void insert(int index, Timestamp value) throws IOException {
        data.insert(index, value.getTime());
    }

    @Override
    public int add(Timestamp value) throws IOException {
        return data.addLong(value.getTime());
    }

    @Override
    public Timestamp get(int index) throws IOException {
        return new Timestamp(data.getLong(index));
    }

    @Override
    public int hash(Timestamp value) {
        return value.hashCode();
    }

    @Override
    public int compare(Timestamp v1, Timestamp v2) {
        return v1.compareTo(v2);
    }

    @Override
    public String toString(Timestamp v) {
        return sdf.format(v);
    }

    @Override
    public Timestamp parse(Object o) {
        if (o instanceof Timestamp) {
            return (Timestamp) o;
        } else if (o instanceof String) {
            try {
                return new Timestamp(sdf.parse((String) o).getTime());
            } catch (ParseException e) {
                throw new IllegalArgumentException(e);
            }
        } else if (o == null) {
            throw new NullPointerException("");
        } else {
            throw new ClassCastException("Expected timestamp, Actual: " + o.getClass());
        }
    }
}
