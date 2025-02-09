package com.quanta.data;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.NoSuchElementException;

public class SmallIntAdapter extends FixedWidthDataAdapter<Integer> {

    public static FixedWidthDataAdapter<Integer> newAdapter(int count) {
        if (count < 256) {
            return new SmallIntAdapter(1);
        } else if (count < 65_535) {
            return new SmallIntAdapter(2);
        } else if (count < 16_777_215) {
            return new SmallIntAdapter(3);
        } else {
            return new IntAdapter();
        }
    }

    final Helper helper;

    public SmallIntAdapter(int width) {
        super(width, false);

        if (width == 1) {
            helper = new OneByte();
        } else if (width == 2) {
            helper = new TwoByte();
        } else if (width == 3) {
            helper = new ThreeByte();
        } else {
            helper = new FourByte();
        }
    }

    @Override
    public int getDataType() {
        return Types.INTEGER;
    }

    @Override
    public void insert(int index, Integer value) throws IOException {
        data.insert(index, helper.toBytes(value));
    }

    @Override
    public int add(Integer value) throws IOException {
        return data.addBytes(helper.toBytes(value));
    }

    @Override
    public Integer get(int index) throws IOException {
        return getInt(index);
    }
    @Override
    public int getInt(int index) throws IOException {
        return helper.toInt(data.getBytes(index));

//        if (cache.containsKey(index))
//            return cache.get(index);
//        cache.put(index, val);
//        return val;
    }

    @Override
    public byte[] toBytes(Integer value) {
        return helper.toBytes(value);
    }

    @Override
    public boolean match(byte[] rawData, byte[] toCompare, int index) {
        return helper.match(rawData, toCompare, index * width);
    }

    @Override
    public int hash(Integer value) {
        return value;
    }

    @Override
    public int compare(Integer v1, Integer v2) {
        return v1.compareTo(v2);
    }

    @Override
    public Integer parse(Object o) {
        if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof String) {
            return Integer.parseInt((String) o);
        } else if (o == null) {
            throw new NullPointerException("");
        } else {
            throw new ClassCastException("Expected int, Actual: " + o.getClass());
        }
    }

    interface Helper {
        byte[] toBytes(int i);

        int toInt(byte v[]);

        boolean match(byte[] rawData, byte[] toCompare, int offset);
    }

    static class OneByte implements Helper {
        @Override
        public byte[] toBytes(int i) {
            return new byte[] {(byte) i};
        }

        @Override
        public int toInt(byte[] v) {
            return Byte.toUnsignedInt(v[0]);
        }

        @Override
        public boolean match(byte[] rawData, byte[] toCompare, int offset) {
            return rawData[offset] == toCompare[offset];
        }
    }

    static class TwoByte implements Helper {
        @Override
        public byte[] toBytes(int i) {
            return new byte[] {
                    (byte)i,
                    (byte)(i >> 8)
            };
        }

        @Override
        public int toInt(byte[] v) {
            return (v[1] & 0xFF) << 8 | (v[0] & 0xFF);
        }

        @Override
        public boolean match(byte[] rawData, byte[] toCompare, int offset) {
            int i2 = offset + 1;
            return rawData[offset] == toCompare[offset] && rawData[i2] == toCompare[i2];
        }
    }

    static class ThreeByte implements Helper {
        @Override
        public byte[] toBytes(int i) {
            return new byte[] {
                    (byte)i,
                    (byte)(i >> 8),
                    (byte)(i >> 16)
            };
        }

        @Override
        public int toInt(byte[] v) {
            return (v[2] & 0xFF) << 16 | (v[1] & 0xFF) << 8 | (v[0] & 0xFF);
        }

        @Override
        public boolean match(byte[] rawData, byte[] toCompare, int offset) {
            boolean match = true;
            for (int j = 0; j < 3; j++) {
                if (rawData[offset + j] != toCompare[j]) {
                    match = false;
                    break;
                }
            }
            return match;
        }
    }

    static class FourByte implements Helper {
        @Override
        public byte[] toBytes(int i) {
            return new byte[] {
                    (byte)i,
                    (byte)(i >> 8),
                    (byte)(i >> 16),
                    (byte)(i >> 24)
            };
        }

        @Override
        public int toInt(byte[] v) {
            return (v[3] & 0xFF) << 24 | (v[2] & 0xFF) << 16 | (v[1] & 0xFF) << 8 | (v[0] & 0xFF);
        }

        @Override
        public boolean match(byte[] rawData, byte[] toCompare, int offset) {
            boolean match = true;
            for (int j = 0; j < 3; j++) {
                if (rawData[offset + j] != toCompare[j]) {
                    match = false;
                    break;
                }
            }
            return match;
        }
    }
//
//    private class LookupCache {
//        private final int capacity;
//        private final int[] keys;
//        private final int[] values;
//        // To mark whether a slot contains a valid entry.
//        private final boolean[] valid;
//
//        public LookupCache(int capacity) {
//            this.capacity = capacity;
//            this.keys = new int[capacity];
//            this.values = new int[capacity];
//            this.valid = new boolean[capacity];
//        }
//
//        private int hash(int key) {
//            return (key % capacity + capacity) % capacity;
//        }
//
//        public void put(int key, int value) {
//            int index = hash(key);
//            keys[index] = key;
//            values[index] = value;
//            valid[index] = true;
//        }
//
//        public int get(int key) {
//            int index = hash(key);
//            if (!valid[index] || keys[index] != key) {
//                throw new NoSuchElementException("Key not found: " + key);
//            }
//            return values[index];
//        }
//
//        public boolean containsKey(int key) {
//            int index = hash(key);
//            return valid[index] && keys[index] == key;
//        }
//    }
}
