package com.quanta.data;

import java.io.IOException;
import java.sql.Types;
import java.util.regex.Pattern;

public class FixedStringAdapter extends FixedWidthDataAdapter<String> {
	private final static Pattern RTRIM = Pattern.compile("\\s+$");

	public static String rtrim(String s) {
	    return RTRIM.matcher(s).replaceAll("");
	}
	
    public FixedStringAdapter(int width) {
        super(width, true);
    }

    @Override
    public int add(String value) throws IOException {
        return data.addBytes(value == null ? new byte[0] : value.getBytes());
    }

    @Override
    public int getDataType() {
        return Types.VARCHAR;
    }

    @Override
    public void insert(int index, String value) throws IOException {
        data.insert(index, value == null ? new byte[0] : value.getBytes());
    }
    @Override
    public String get(int index) throws IOException {
        return rtrim(new String(data.getBytes(index)));
    }

    @Override
    public int hash(String value) {
        return value == null ? 0 : value.hashCode();
    }

    @Override
    public int compare(String v1, String v2) {
        return v1.compareTo(v2);
    }


    @Override
    public String parse(Object o) {
        return String.valueOf(o);
    }

}
