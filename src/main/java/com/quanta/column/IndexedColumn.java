package com.quanta.column;


import com.quanta.data.*;
import com.quanta.util.JSONWriter;
import com.quanta.blob.FixedRegion;
import com.quanta.util.ByteBitSet;
import com.quanta.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import java.util.logging.Logger;

public class IndexedColumn<T> extends SortedColumn<T> {

    private static final Logger logger = Logger.getLogger(IndexedColumn.class.getName());

    private final int maxUnique;
    private final FixedWidthDataAdapter<Integer> rows;
    private final FixedRegion bitmap;
    private int uniques;

    public IndexedColumn(String name, String file, DataAdapter<T> adapter, int maxUnique) throws IOException {
        super(name, adapter, maxUnique);

        this.maxUnique = maxUnique;
        this.rows = SmallIntAdapter.newAdapter(maxUnique);
        this.bitmap = new FixedRegion(8);
        init(file, sortedValues.regions()[0], rows.regions()[0], bitmap);

        uniques = super.size();

        optimalBitmap();
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public int getColumnType() {
        return 1;
    }

    @Override
    public void add(T value) throws IOException {
        lock.lock();

        try {
            // find id from sorted data
            int valueId  = 0;
            int sortedId = search(value);

            if (sortedId < 0) {
                // if does not exist in sorted data
                // then add to it and sort data
                valueId = values.add(value);
                sort(value, valueId, false);

                if (sortedValues.size() != values.size()) {
                    throw new IllegalStateException("Something went wrong");
                }
                uniques++;
            } else {
                valueId = sortedValues.get(sortedId);
            }

            rows.add(valueId);
        } catch (ClassCastException cce) {
            //logger.info("Unable to add {} to column {} due to class cast exception", value, name, cce);
            throw cce;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T get(int index) throws IOException {
        int uid = rows.getInt(index);
        return values.get(uid);
    }

    @Override
    public void writeMeta(JSONWriter json) throws IOException {
        json.newObject();

        json.write("name", name);
        json.write("index",   "yes");
        json.write("is_fact", "false", false);
        json.write("data", values.isString ? "text" : "int");

        json.newArray("values");

        boolean quotes = values.isString;

        if (uniques < 10_000) {
            for (int i = 0; i < uniques; i++) {
                json.writeValue(values.toString(values.get(i)), quotes);
            }
        }

        json.closeArray();

        json.closeObject();
    }



    /////////////////

    @Override
    protected void forValueId(ByteBitSet set, int valueId) throws IOException {
        // Compute the base offset for this unique index in the bitmap.
        int baseIndex = valueId * bit_words;
        byte[] valueIdInBytes = rows.toBytes(valueId);

        // Loop over each 64‐bit word in the bitmap for this index.
        for (int y = bit_words - 1; y >= 0; y--) {
            long word = bitmap.getLong(baseIndex + y);
            if (word != 0) {
                // Each word covers 64 blocks; each block represents bit_length rows.
                int blockStartOffset = y * 64 * bit_length;

                // Use bit–tricks to iterate only over the set bits in the word.
                while (word != 0) {
                    // Find the position (0..63) of the lowest set bit.
                    int bitPos = Long.numberOfTrailingZeros(word);
                    // Clear that lowest set bit.
                    word &= word - 1;

                    // Compute the starting row index for this block.
                    int blockStart = blockStartOffset + (bitPos * bit_length);
                    // Determine the end index for this block.
                    int blockEnd = Math.min(blockStart + bit_length, size());
                    int count = blockEnd - blockStart;

                    // Read the contiguous block of raw bytes.
                    byte[] rawData = rows.getRawBytes(blockStart, count);
                    // Process the block in one go.
                    processBlockRaw(rawData, blockStart, count, valueIdInBytes, rows.width, set);
//                    // Retrieve all the values for this block at once.
//                    int[] blockValues = rows.getInts(blockStart, count);
//
//                    // Now, compare all values in the block to the provided index.
//                    for (int i = 0; i < count; i++) {
//                        if (blockValues[i] == valueId) {
//                            set.set(blockStart + i);
//                        }
//                    }
                }
            }
        }
    }

    /**
     * Scans the given raw byte array (representing a contiguous block of rows)
     * for occurrences of the target value (given as raw bytes), and marks the corresponding
     * row IDs in the result ByteBitSet.
     *
     * @param rawData      The raw byte array; its length should be (count * width).
     * @param blockStartRow The starting row number corresponding to the first value in rawData.
     * @param count        The number of rows (values) contained in rawData.
     * @param targetRaw    The raw bytes (length == width) representing the target value.
     * @param width        The number of bytes per value.
     * @param set          The result ByteBitSet in which to mark matching row IDs.
     */
    private void processBlockRaw(byte[] rawData, int blockStartRow, int count,
                                 byte[] targetRaw, int width, ByteBitSet set) {
        // Loop over each value in the block.
        for (int i = 0; i < count; i++) {
            int offset = i * width;
            boolean match = true;
            // Compare the bytes for this value with targetRaw.
            for (int j = 0; j < width; j++) {
                if (rawData[offset + j] != targetRaw[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                // If there's a match, mark the corresponding row.
                set.set(blockStartRow + i);
            }
        }
    }

    private void optimalBitmap() {
        int maxBytes = 40 * 1024 * 1024;  // 40 MB
        int size = rows.size();

        // Compute the minimal range required:
        double required = ((double) size * uniques) / (maxBytes * 8.0);
        // Round up to the next multiple of 50:
        int range = (int) (Math.ceil(required / 50.0) * 50);
        if (range < 50) {
            range = 50;
        }

        int bits = size / range;
        int bitWords = (bits + 63) / 64;

        this.bit_length = range;
        this.bit_words = bitWords;
    }


    private int bit_length, bit_words;

    public void rebuild() throws IOException {
        bitmap.reset();

        if (maxUnique > 30000 && uniques > maxUnique) {
            logger.warning("Column: " + this.name + " has more unique values: " + uniques + " than max: " + maxUnique);
            return;
        }

        // Compute optimal bitmap parameters in one step.
        optimalBitmap();

        // Allocate space for the bitmap: one contiguous block for each unique value,
        // each needing 'bit_words' 64-bit longs.
        int total = uniques * bit_words;
        for (int i = 0; i < total; i++) {
            bitmap.addLong(0L);
        }

        int size = rows.size();
        int x, y;
        // Cache previous unique value and block index to avoid repeated lookups.
        int px = -1, py = -1;
        long word = 0;
        int last = 0;

        // Process rows in reverse order.
        for (int i = size - 1; i >= 0; i--) {
            x = rows.getInt(i);      // unique value index
            y = i / bit_length;      // block number

            // Only update the word if either the unique value or block has changed.
            if (x != px || y != py) {
                px = x;
                py = y;
                // Compute the location in the bitmap.
                // The bitmap is organized by unique value, each with 'bit_words' words.
                // The index into the bitmap is: (x * bit_words) + (y / 64)
                last = bitmap.rp(x * bit_words + (y / 64));
                word = bitmap.longAt(last);
            }

            // Set the bit corresponding to the row's block.
            word |= (1L << (y % 64));
            bitmap.replace(last, word);
        }
    }


    //private long word(int uid, )


    public static void main(String[] args) throws IOException {
        new File(Utils.USER_HOME + "/temp/ic").delete();

//        IndexedColumn ic = new IndexedColumn("a", Utils.USER_HOME + "/temp/ic", new Dictionary(5), 20);
//
//        String symbols[] = new String[] {"SPY", "QQQ", "GS", "GOOG", "XLF", "BAC", "HPQ", "GM", "FCX", "GLD"};
//        ThreadLocalRandom rand = ThreadLocalRandom.current();
//
//        for (int i = 0; i < 150; i++) {
//            ic.add(symbols[rand.nextInt(0, symbols.length)]);
//        }
//        System.out.println(ic.get(130));
//        for (int i = 0; i < 10; i++) {
//            System.out.println(ic.get(i));
//        }


        IndexedColumn intColumn = new IndexedColumn("", Utils.USER_HOME + "/temp/ic", new IntAdapter(), 20);
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = 0; i < 150; i++) {
            intColumn.add(rand.nextInt(0, 20));
        }
        System.out.println(intColumn.get(130));
        for (int i = 0; i < 10; i++) {
            System.out.println(intColumn.get(i));
        }
    }
}
