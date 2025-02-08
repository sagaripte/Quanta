package com.quanta.blob;

import com.quanta.util.Utils;

import java.io.File;
import java.io.IOException;

public class VariableRegion extends Region {

    int curr;
    int next, start, end;

    public VariableRegion() {
        this(4 * 1024 * 1024); // 4mb
    }

    public VariableRegion(int width) {
        super(width);

        this.curr  = 0;
        this.start = 0;
        this.end   = block_width;
    }

    @Override
    protected void create() throws IOException {
        alloc();
        next = blocks[0];
        blob.putLong(header_pos, next);
    }

    @Override
    protected void read() throws IOException {
        next = blob.getInt(header_pos);
        super.read();
    }

    private int cp(int width) throws IOException {
        int e = next + width;
        if (e >= start && e < end) {
            int r = next;
            next = e;
            blob.putInt(header_pos, next);
            return r;
        }

        int block_id = block_count;
        alloc();

        curr  = block_id;
        start = blocks[curr];
        end   = start + block_width;

        next = start + width;
        blob.putLong(header_pos, next);

        return start;
    }

    /*

    public long addInt(int v) throws IOException {
        long p = cp(4);
        blob.putInt(p, v);
        return p;
    }

    public int getInt(long pos) {
        return blob.getInt(pos);
    }

    public long addLong(long v) throws IOException {
        long p = cp(8);
        blob.putLong(p, v);
        return p;
    }

    public long getLong(int index) {
        return blob.getLong(rp(index));
    }

    public long addByte(byte b) throws IOException {
        long p = cp(1);
        blob.putByte(p, b);
        return p;
    }

    public byte getByte(int index) {
        return blob.getByte(rp(index));
    }

    public void addBoolean(boolean b) throws IOException {
        blob.putBoolean(cp(), b);
    }

    public boolean getBoolean(int index) {
        return blob.getBoolean(rp(index));
    }
    */

    public int addString(String s) throws IOException {
        byte[] b = s == null ? new byte[0] : s.getBytes();

        return addBytes(b);
    }

    public String getString(int pos) throws IOException {
        return new String(getBytes(pos));
    }

    public int addBytes(byte[] v) throws IOException {
        int p = cp(v.length + 4);

        blob.putInt(p, v.length);
        blob.putBytes(p + 4, v);

        return p;
    }

    public byte[] getBytes(int pos) throws IOException {
        int len = blob.getInt(pos);
        return blob.getBytes(pos + 4, len);
    }

    public static void main(String[] args) throws IOException {
        // offline: Time to add : 57728
        // memory: Time to add : 182
        new File(Utils.USER_HOME + "/temp/vr_1").delete();

        FixedRegion fr = new FixedRegion(4);
        VariableRegion vr = new VariableRegion();

        Blob blob = new MemoryBlob(Utils.USER_HOME + "/temp/vr_1", fr, vr);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            int p = vr.addString("Label " + i);
            fr.addLong(p);
            if (i % 200 == 0)
                System.out.println(">>>>> SIZE: " + i);
        }
        System.out.println("Time to add : " + (System.currentTimeMillis() - start));

        for (int i = 995; i < 1000; i++) {
            System.out.println(vr.getString(fr.getInt(i)));
        }
    }
}
