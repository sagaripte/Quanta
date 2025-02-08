package com.quanta.column;


import com.quanta.util.JSONWriter;
import com.quanta.blob.FixedRegion;
import com.quanta.data.DataAdapter;
import com.quanta.data.Dictionary;
import com.quanta.data.FixedWidthDataAdapter;
import com.quanta.data.SmallIntAdapter;
import com.quanta.util.ByteBitSet;
import com.quanta.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import java.util.logging.Logger;

public class IndexedColumn<T> extends SortedColumn<T> {

    private static final Logger logger = Logger.getLogger(IndexedColumn.class.getName());

    private final int maxUnique;
    private final FixedWidthDataAdapter<Integer> list;
    private final FixedRegion bitmap;
    private int uniques;

    public IndexedColumn(String name, String file, DataAdapter<T> adapter, int maxUnique) throws IOException {
        super(name, adapter, maxUnique);

        this.maxUnique = maxUnique;
        this.list = SmallIntAdapter.newAdapter(maxUnique);
        this.bitmap = new FixedRegion(8);
        init(file, sorted.regions()[0], list.regions()[0], bitmap);

        uniques = super.size();

        optimalBitmap();
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public int getColumnType() {
        return 1;
    }

    @Override
    public void add(T value) throws IOException {
        lock.lock();

        try {
            int sid = search(value);
            int idx = 0;

            if (sid < 0) {

                idx = adapter.add(value);
                sort(value, idx, false);

                if (sorted.size() != adapter.size()) {
                    throw new IllegalStateException("Something went wrong");
                }
                uniques++;
            } else {
                idx = sorted.get(sid);
            }

            list.add(idx);
        } catch (ClassCastException cce) {
            //logger.info("Unable to add {} to column {} due to class cast exception", value, name, cce);
            throw cce;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T get(int index) throws IOException {
        int uid = list.getInt(index);
        return adapter.get(uid);
    }

    @Override
    public void writeMeta(JSONWriter json) throws IOException {
        json.newObject();

        json.write("name", name);
        json.write("index",   "yes");
        json.write("is_fact", "false", false);
        json.write("data", adapter.isString ? "text" : "int");

        json.newArray("values");

        boolean quotes = adapter.isString;

        if (uniques < 10_000) {
            for (int i = 0; i < uniques; i++) {
                json.writeValue(adapter.toString(adapter.get(i)), quotes);
            }
        }

        json.closeArray();

        json.closeObject();
    }



    /////////////////

    @Override
    protected void forIndex(ByteBitSet set, int index) throws IOException {
        int x = index * bit_words, begin;

       // System.out.println("\n FOR INDEX: " + index);

        for (int y = bit_words - 1; y > -1; y--) {
            long word = bitmap.getLong(x + y);

            if (word != 0) {
                begin = y * 64 * bit_length;

                for (int i = 0; i < 64; i++) {
                    if ((word & (1l << i)) != 0) {
                        int idx = begin + (i * bit_length);
                        int end = Math.min(idx + bit_length, size());

                        for (; idx < end; idx++) {
                            if (list.getInt(idx) == index) {
                                //System.out.print(idx + ", ");
                                set.set(idx);
                            }
                        }
                    }
                }
            }
        }

        //System.out.println();
    }

    private void optimalBitmap() {
        int maxBytes = 40 * 1024 * 1024;
        int range = 0, bits, bytes, total;

        int size  = list.size();

        do {
            range += 50;
            bits   = size / range;
            bytes  = bits / 8;
            total  = bytes * uniques;

            if (total < 0) // int overflow
                total = maxBytes + 1;
        } while (total > maxBytes);


        bit_length = range;
        bit_words  = (bits + 63) / 64;
        //return new int[] {range, (bits + 63) / 64};
    }

    private int bit_length, bit_words;

    public void rebuild() throws IOException {
        bitmap.reset();

        if (maxUnique > 30000 && uniques > maxUnique) {
            logger.warning("Column: " + this.name + " has more unique values: " + uniques + " than max: " + maxUnique);
            return;
        }

        optimalBitmap();

        int total = uniques * bit_words;
        for (int i = 0; i < total; i++) {
            bitmap.addLong(0L);
        }

        int size = list.size();
        int x, y, px = -1, py = -1;
        long word = 0;
        int last = 0;
        for (int i = size - 1; i > -1; i--) {
            x = list.getInt(i);
            y = i / bit_length;

            if (x != px || y != py) {
                px   = x;
                py   = y;
                last = bitmap.rp(x * bit_words + (y / 64));
                word = bitmap.longAt(last);
            }

            word |= (1L << (y % 64));
            bitmap.replace(last, word);
        }
    }

    //private long word(int uid, )


    public static void main(String[] args) throws IOException {
        new File(Utils.USER_HOME + "/temp/ic").delete();

        IndexedColumn ic = new IndexedColumn("a", Utils.USER_HOME + "/temp/ic", new Dictionary(5), 20);

        String symbols[] = new String[] {"SPY", "QQQ", "GS", "GOOG", "XLF", "BAC", "HPQ", "GM", "FCX", "GLD"};
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (int i = 0; i < 150; i++) {
            ic.add(symbols[rand.nextInt(0, symbols.length)]);
        }
        System.out.println(ic.get(130));
        for (int i = 0; i < 10; i++) {
            System.out.println(ic.get(i));
        }
    }
}
