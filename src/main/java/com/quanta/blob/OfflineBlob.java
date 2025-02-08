package com.quanta.blob;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OfflineBlob implements Blob {

    public final String fileName;
    private final RandomAccessFile file;
    //private FileChannel channel;
    public final boolean isNew;

    private final Region regions[];
    private int next;
    private long file_length;

    private Lock lock;

    public OfflineBlob(String file, Region...regions) throws IOException {
        this.isNew      = !new File(file).exists();
        this.fileName   = file;
        this.file       = new RandomAccessFile(file, "rw");
        //this.channel    = this.file.getChannel();
        this.regions    = regions;
        this.lock       = new ReentrantLock();

        init();
    }

    private void init() throws IOException {
        file_length = 8 * 1024 * 1024;
        open();

        int header_pos = 0;

        for (int i = 0; i < regions.length; i++) {
            Region r = regions[i];
            r.blob = this;
            header_pos  += 16;
            r.header_pos = header_pos;
        }

        if (isNew) {
            next = header_pos + 16;
            putLong(0, next);

            for (int i = 0; i < regions.length; i++) {
                regions[i].create();
            }
        } else {
            next = getInt(0);

            for (int i = 0; i < regions.length; i++) {
                regions[i].read();
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
            putLong(0, next);

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
            file.setLength(file_length);

            open();
        } finally {
            lock.unlock();
        }
    }

    private void open() throws IOException {
        //base = map(channel, false, 0, file_length);
    }

    public void close() throws IOException {
        /*if (base > 0l) {
            unmap(channel, base, file_length);
            base = -1;
        }*/
    }

    public void putInt(int pos, int v) throws IOException {
        file.seek(pos);
        file.writeInt(v);
    }

    public int getInt(int pos) throws IOException {
        file.seek(pos);
        return file.readInt();
    }

    public void putLong(int pos, long v) throws IOException {
        file.seek(pos);
        file.writeLong(v);
    }

    public long getLong(int pos) throws IOException {
        file.seek(pos);
        return file.readLong();
    }

    public void putByte(int pos, byte b) throws IOException {
        file.seek(pos);
        file.writeByte(b);
    }

    public byte getByte(int pos) throws IOException {
        file.seek(pos);
        return file.readByte();
    }

    public void putBoolean(int pos, boolean b) throws IOException {
        file.seek(pos);
        file.writeBoolean(b);
    }

    public boolean getBoolean(int pos) throws IOException {
        file.seek(pos);
        return file.readBoolean();
    }

    public void putBytes(int pos, byte[] v) throws IOException {
        file.seek(pos);
        file.write(v);
    }

    public byte[] getBytes(int pos, int width) throws IOException {
        file.seek(pos);
        byte[] v = new byte[width];
        file.read(v);

        return v;
    }

    public void copy(int from, int to, int width) throws IOException {
        //TODO://
    }

}
