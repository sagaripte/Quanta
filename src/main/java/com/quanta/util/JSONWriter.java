package com.quanta.util;


import java.io.ByteArrayOutputStream;
import java.util.Arrays;

public class JSONWriter extends ByteArrayOutputStream {
    private static final byte[] quote = "\"".getBytes();
    private static final byte[] semi  = ":".getBytes();
    private static final byte[] sep   = ",".getBytes();
    private static final byte[] os   = "{".getBytes();
    private static final byte[] oe   = "}\n,".getBytes();
    private static final byte[] as   = "[".getBytes();
    private static final byte[] ae   = "]\n,".getBytes();

    //private int indent;

    public JSONWriter() {
    }

    public byte[] buffer() {
        return buf;
    }

    public int len() {
        return count - 1;
    }

    public synchronized byte toByteArray()[] {
        count--; // -= 2;
        return Arrays.copyOf(buf, count);
    }

    public void newObject() {
        write(os, 0, 1);
    }
    public void closeObject() {

        if (buf[count - 1] != os[0]) {
            count--; // -= 2;
        }
        write(oe, 0, oe.length);
    }

    public void newArray(String key) {
        writeKey(key);
        write(as, 0, 1);
    }

    public void newArray() {
        write(as, 0, 1);
    }
    public void closeArray() {
        if (buf[count - 1] != as[0]) {
            count--; // -= 2;
        }
        write(ae, 0, ae.length);
    }

    public void write(String key, String value) {
        write(key, value, true);
    }

    public void write(String key, int value) {
        write(key, Integer.toString(value).getBytes(), false);
    }

    public void write(String key, boolean value) {
        write(key, Boolean.toString(value).getBytes(), false);
    }

    public void write(String key, double value) {
        write(key, Double.toString(value).getBytes(), false);
    }

    private void writeKey(String key) {
        writeOne(quote);
        write(key);
        writeOne(quote);
        writeOne(semi);
    }
    public void write(String key, String val, boolean inQuotes) {
        write(key, inQuotes ? quote(val).getBytes() : val.getBytes(), false);
    }

    public void write(String key, byte value[], boolean inQuotes) {
        writeKey(key);
        writeValue(value, inQuotes);
    }

    public void writeValue(String value, boolean inQuotes) {
        writeValue(inQuotes ? quote(value).getBytes() : value.getBytes(), false);
    }

    public void writeValue(byte value[], boolean inQuotes) {
        if (inQuotes)
            writeOne(quote);

        write(value, 0, value.length);

        if (inQuotes)
            writeOne(quote);

        writeOne(sep);
    }

    private void writeOne(byte b[]) {
        write(b, 0, 1);
    }

    private void write(String s) {
        byte b[] = s.getBytes();
        write(b, 0, b.length);
    }

    public static void main(String[] args) {
        JSONWriter out = new JSONWriter();

        out.newObject();

        out.write("key1", "value");
        out.write("key2", 2123);

        out.closeObject();

        System.out.println(out.toString());

    }

    public static String quote(String string) {
        if (Utils.isEmpty(string)) {
            return "\"\"";
        }

        char         c = 0;
        int          i;
        int          len = string.length();
        StringBuilder sb = new StringBuilder(len + 4);
        String       t;

        sb.append('"');
        for (i = 0; i < len; i += 1) {
            c = string.charAt(i);
            switch (c) {
                case '\\':
                case '"':
                    sb.append('\\');
                    sb.append(c);
                    break;
                case '/':
                    //                if (b == '<') {
                    sb.append('\\');
                    //                }
                    sb.append(c);
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    //sb.append("\\n");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\r':
                    //sb.append("\\r");
                    break;
                default:
                    if (c < ' ') {
                        t = "000" + Integer.toHexString(c);
                        sb.append("\\u" + t.substring(t.length() - 4));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
