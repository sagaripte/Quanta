package com.quanta.util;

import java.util.Arrays;

public class ByteBitSet {

    long[] data;

    public ByteBitSet(int sizeInBits) {
        data = new long[(sizeInBits + 63) / 64];
    }

//    public void fill(long l) {
//        Arrays.fill(data, l);
//    }

    public void clear() {
        Arrays.fill(data, 0);
    }

    public void resize(int sizeInBits) {
        data = Arrays.copyOf(data, (sizeInBits + 63) / 64);
    }

    public int cardinality() {
        int sum = 0;
        for(long l : data)
            sum += Long.bitCount(l);
        return sum;
    }

    public void replace(ByteBitSet another) {
        this.data = another.data;
    }

    public boolean get(int i) {
        return (data[i / 64] & (1L << (i % 64))) !=0;
    }
    public void set(int i) {
        data[i / 64] |= (1L << (i % 64));
    }

    public void unset(int i) {
        data[i / 64] &= ~(1L << (i % 64));
    }

    public void set(int i, boolean b) {
        if(b) set(i); else unset(i);
    }

    // for(int i=bs.nextSetBit(0); i>=0; i=bs.nextSetBit(i+1)) { // operate on
    // index i here }
    public int nextSetBit(int i) {
        int x = i / 64;
        if(x>=data.length) return -1;
        long w = data[x];
        w >>>= (i % 64);
        if (w != 0) {
            return i + Long.numberOfTrailingZeros(w);
        }
        ++x;
        for (; x < data.length; ++x) {
            if (data[x] != 0) {
                return x * 64 + Long.numberOfTrailingZeros(data[x]);
            }
        }
        return -1;
    }

    public void and(int pos, long word) {
        data[pos] &= word;
    }

    public void xor(int pos, long word) {
        data[pos] ^= word;
    }



    public void and(ByteBitSet another) {
        long[] o = another.data;

        for (int i = data.length - 1; i > -1 ; i--) {
            data[i] &= o[i];
        }
    }
    public void xor(ByteBitSet another) {
        long[] o = another.data;

        for (int i = data.length - 1; i > -1 ; i--) {
            data[i] ^= o[i];
        }
    }
    public void not() {
        for (int i = data.length - 1; i > -1 ; i--) {
            data[i] = ~data[i];
        }
    }

    public interface BitOperator {
        default void doWork(ByteBitSet set, int index) {
            set.set(index);
        }
        void doWork(ByteBitSet set, int unit, byte bits);
    }

    public static final class AndOperator implements BitOperator {
        @Override
        public void doWork(ByteBitSet set, int unit, byte bits) {
            set.and(unit, bits);
        }
    }

    public static final class XOROperator implements BitOperator {
        @Override
        public void doWork(ByteBitSet set, int unit, byte bits) {
            set.xor(unit, bits);
        }
    }

    public static void main(String[] args) {
        ByteBitSet set = new ByteBitSet(10000);
        
        set.set(5);
        set.set(34);
        set.set(782);
        set.set(2343);
        set.set(3002);


        for (int i = set.nextSetBit(0); i > -1; i = set.nextSetBit(i + 1)) {
            System.out.println(i);
        }

        
    }
}
