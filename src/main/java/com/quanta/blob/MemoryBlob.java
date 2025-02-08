package com.quanta.blob;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryBlob implements Blob {

    public final String fileName;
    private final RandomAccessFile raf;
    private FileChannel channel;
    public final boolean isNew;

    private final Region[] regions;
    //private long base;
    private MappedByteBuffer bb;
    private int next;
    private long file_length;

    private final Lock lock;

    public MemoryBlob(String file, Region...regions) throws IOException {
        File temp = new File(file);

        this.isNew      = !temp.exists();
        this.fileName   = file;
        this.raf        = new RandomAccessFile(file, "rw");

        this.regions    = regions;
        this.lock       = new ReentrantLock();

        init();
    }

    private void init() throws IOException {
        open();

        int header_pos = 0;

        for (Region r : regions) {
            r.blob = this;
            header_pos += 16;
            r.header_pos = header_pos;
        }

        if (isNew) {
            next = header_pos + 16;
            bb.putInt(0, next);
            //unsafe.putLong(base, next);

            for (Region region : regions) {
                region.create();
            }
        } else {
            next = bb.getInt(0);

            for (Region region : regions) {
                region.read();
            }
        }
    }

    @Override
    public int alloc(int bytes) throws IOException {
        lock.lock();

        try {
            int total = next + bytes;

            if (total > file_length) {
                extend();
            }

            int pos = next;
            next = total;
            bb.putLong(0, next);
            //unsafe.putLong(base, next);

            return pos;
        } finally {
            lock.unlock();
        }
    }

    private void extend() throws IOException {
        lock.lock();

        try {
            close();
            file_length += (8 * 1024 * 1024); // 4mb
            raf.setLength(file_length);

            open();
        } finally {
            lock.unlock();
        }
    }

    private void open() throws IOException {
        file_length = raf.length();
        if (file_length == 0)
            file_length = 8 * 1024 * 1024;

        this.channel = FileChannel.open(Paths.get(fileName), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        this.bb = channel.map(FileChannel.MapMode.READ_WRITE, 0, file_length);
    }

    public void close() throws IOException {
        try {
            if (bb == null || !bb.isDirect())
                return;

            bb.force();

            CLEAN.invoke(UNSAFE, bb);
            channel.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void check(long pos, int v) throws IOException {
        //System.out.println(pos + v);
        if (pos + v >= file_length)
            throw new IOException("Requested pointer greater than file length " + (pos + v) + " vs " + file_length);
    }
    public void putInt(int pos, int v) throws IOException {
        //System.out.println(pos + " " + v);
        //System.out.println("put_int(" + pos + ", " + v + ")");
        check(pos, 4);
        bb.putInt(pos, v);
        //unsafe.putInt(base + pos, v);
    }

    public int getInt(int pos) throws IOException {
        check(pos, 4);
        return bb.getInt(pos); //  unsafe.getInt(base + pos);
    }

    public void putLong(int pos, long v) throws IOException {
        //System.out.println("put_long(" + pos + ", " + v + ")");
        check(pos, 8);
        bb.putLong(pos, v);
        //unsafe.putLong(base + pos, v);
    }

    public long getLong(int pos) throws IOException {
        check(pos, 8);
        return bb.getLong(pos);
        //return unsafe.getLong(base + pos);
    }

    public void putByte(int pos, byte b) throws IOException {
        check(pos, 1);
        bb.put(pos, b);
        //unsafe.putByte(base + pos, b);
    }

    public byte getByte(int pos) throws IOException {
        check(pos, 1);
        return bb.get(pos);
        //return unsafe.getByte(base + pos);
    }

    public void putBoolean(int pos, boolean b) throws IOException {
        putByte(pos, (byte) (b ? 0 : 1));
//        check(pos, 1);
//
//        unsafe.putByte(base + pos, (byte) (b ? 0 : 1));
    }

    public boolean getBoolean(int pos) throws IOException {
        return getByte(pos) == 0;
//        check(pos, 1);
//        return unsafe.getByte(base + pos) == 0;
    }

    public void putBytes(int pos, byte[] v) throws IOException {
        check(pos, v.length);
        bb.put(pos, v);
        //unsafe.copyMemory(v, BYTE_ARRAY_OFFSET, null, base + pos, v.length);
    }

    public byte[] getBytes(int pos, int width) throws IOException {
        check(pos, width);
        byte[] v = new byte[width];
        bb.get(pos, v);
        //unsafe.copyMemory(null, base + pos, v, BYTE_ARRAY_OFFSET, width);
        return v;
    }

    public void copy(int from, int to, int width) throws IOException {
        byte[] data = getBytes(from, width);
        putBytes(to, data);
        //unsafe.copyMemory(null, base + from, null, base + to, width);
    }


    private static final Method CLEAN;
    private static final Object UNSAFE;

    static {
        try {
            Class<?> unsafeClass;
            try {
                unsafeClass = Class.forName("sun.misc.Unsafe");
            } catch (Exception ex) {
                unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
            }
            Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            clean.setAccessible(true);
            Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            Object theUnsafe = theUnsafeField.get(null);

            CLEAN = clean;
            UNSAFE = theUnsafe;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
