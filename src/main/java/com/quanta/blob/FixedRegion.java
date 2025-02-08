package com.quanta.blob;


import com.quanta.util.Utils;

import java.io.File;
import java.io.IOException;

public class FixedRegion extends Region {
    private static final int SIZE = 500;

    final int width;
    int size;
    int curr, start, end;

    public FixedRegion(int width) {
        super(width * SIZE);
        this.width = width;
        this.size  = 0;
        this.curr  = 0;
        this.start = 0;
        this.end   = SIZE;
    }

    public void reset() throws IOException {
        this.size  = 0;
        this.curr  = 0;
        this.start = 0;
        this.end   = SIZE;
        blob.putInt(header_pos, size);
    }

    public int size() {
        return size;
    }

    @Override
    protected void create() throws IOException {
        blob.putInt(header_pos, size);
        alloc();
    }

    @Override
    protected void read() throws IOException {
        super.read();

        size  = blob.getInt(header_pos);

        curr  = block_count == 0 ? block_count : block_count - 1;
        start = curr * SIZE;
        end   = start + SIZE;
    }

    public int rp(int i) {
        //System.out.print((i / 100) + " " + (i % 100) + "  ");
        if (i >= size)
            throw new ArrayIndexOutOfBoundsException("size: " + size + ", index: " + i);

        return blocks[i / SIZE] + ((i % SIZE) * width);
    }

    private int cp() throws IOException {
        if (size >= start && size < end) {
            return inc();
        }

        int block_id = size / SIZE;

        if (block_id >= block_count)
            alloc();

        curr = block_id;
        start = block_id * SIZE;
        end   = start + SIZE;

        return inc();
    }


    private int inc() throws IOException {
        int block_pos = (size % SIZE) * width;
        size++;
        blob.putInt(header_pos, size);

//        if (size % 50 == 0)
//            System.out.println("Updated size: " + blob.getInt(header_pos));

        return blocks[curr] + block_pos;
    }

    public void make_space(int pos) throws IOException {
        int block = pos / SIZE;
        int mod   = pos % SIZE;

        byte[] carry = make_pos(block, mod, 1);

        int newPosBlockId = (size + 1) / SIZE;

        while (carry.length > 0) {
            block++;
            mod = 0;

            if (block == block_count && newPosBlockId >= block) {
                alloc();
            }

            byte[] newCarry = make_pos(block, mod, 1);
            blob.putBytes(blocks[block], carry);
            carry = newCarry;
        }

        size++;
        blob.putInt(header_pos, size);

        if (block > curr) {
            curr = block;
            start = block * SIZE;
            end   = start + SIZE;
        }
    }

    private byte[] make_pos(int block, int from, int how_many) throws IOException {
        int plus  = from + how_many;
        int minus = SIZE - how_many; // block + 1 == block_count ? ((size - 1) % SIZE) - how_many :
        int len   = SIZE - from - how_many;

        if (block + 1 == block_count) {
            int m = size % SIZE;
            if ((size - 1) / SIZE < block) { // in new block, for the last item from prev
                minus = -1;
                len = 0;
            } else if (m == 0) { // last item in block
                minus = SIZE - how_many;
            } else { // in between
                minus = -1;
                len = m - how_many + 1;
            }
        }

        //System.out.println("block: " + block + ", mod: " + from + ", plus: " + plus + ", minus: " + minus);

        byte[] carry = new byte[0];

        if (minus > 0) {
           // long carryFrom = (minus * width);
           // System.out.println("   carry: " + minus + ", to: " + (minus + how_many) + ", t: " + blob.getInt(blocks[block] + carryFrom));

            carry = blob.getBytes(blocks[block] + (minus * width), how_many * width);
        }

        //long from = from * width;
        //long to   = (plus * width);
        //System.out.println("    copy: " + from + ", to: " + plus + ", len: " + len);
        blob.copy(blocks[block] + from * width, blocks[block] + (plus * width), len * width);

        return carry;
    }

    public void insert(int index, int v) throws IOException {
        make_space(index);
        blob.putInt(rp(index), v);
    }

    public void insert(int index, long v) throws IOException {
        make_space(index);
        blob.putLong(rp(index), v);
    }

    public void insert(int index, byte v) throws IOException {
        make_space(index);
        blob.putByte(rp(index), v);
    }

    public void insert(int index, boolean v) throws IOException {
        make_space(index);
        blob.putBoolean(rp(index), v);
    }

    public void insert(int index, byte v[]) throws IOException {
        make_space(index);
        blob.putBytes(rp(index), v);
    }

    public int addInt(int v) throws IOException {
        int s = size;
        blob.putInt(cp(), v);
        return s;
    }

    public int getInt(int index) throws IOException {
        return blob.getInt(rp(index));
    }

    public int addLong(long v) throws IOException {
        int s = size;
        blob.putLong(cp(), v);
        return s;
    }

    public long getLong(int index) throws IOException {
        return blob.getLong(rp(index));
    }

    public long longAt(int pos) throws IOException {
        return blob.getLong(pos);
    }

    public void replace(int pos, long newValue) throws IOException {
        blob.putLong(pos, newValue);
    }

    public int addByte(byte b) throws IOException {
        int s = size;
        blob.putByte(cp(), b);
        return s;
    }

    public byte getByte(int index) throws IOException {
        return blob.getByte(rp(index));
    }

    public int addBoolean(boolean b) throws IOException {
        int s = size;
        blob.putBoolean(cp(), b);
        return s;
    }

    public boolean getBoolean(int index) throws IOException {
        return blob.getBoolean(rp(index));
    }

    public int addBytes(byte v[]) throws IOException {
        int s = size;
        blob.putBytes(cp(), v);
        return s;
    }

    public byte[] getBytes(int index) throws IOException {
        return blob.getBytes(rp(index), width);
    }

    public static void main(String[] args) throws IOException {
        new File(Utils.USER_HOME + "/temp/fr_1").delete();

        FixedRegion fr = new FixedRegion(4);
        MemoryBlob blob = new MemoryBlob(Utils.USER_HOME + "/temp/fr_1", fr);

        for (int i = 0; i < 300; i++) {
            fr.addInt(i);
        }

        for (int i = 295; i < fr.size; i++) {
            System.out.println(i + "\t" + fr.getInt(i));
        }
        System.out.println();

        fr.insert(52, 2);

        for (int i = 95; i < 105; i++) {
            System.out.println(i + "\t" + fr.getInt(i));
        }
//        System.out.println();
//        for (int i = 195; i < 205; i++) {
//            System.out.println(i + "\t" + fr.getInt(i));
//        }
//        System.out.println();
//        for (int i = 295; i < fr.size; i++) {
//            System.out.println(i + "\t" + fr.getInt(i));
//        }
        //System.out.println(fr.getInt(fr.size - 1));

        blob.close();

        // reopen test

        System.out.println("\n\n Reopening Blob \n\n");

        blob = new MemoryBlob(Utils.USER_HOME + "/temp/fr_1", fr);


        for (int i = 95; i < 105; i++) {
            System.out.println(i + "\t" + fr.getInt(i));
        }

    }
}