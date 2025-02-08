package com.quanta.blob;

import java.io.Closeable;
import java.io.IOException;

public interface Blob extends Closeable {

    int alloc(int bytes) throws IOException;

    void putInt(int pos, int v) throws IOException;

    int getInt(int pos) throws IOException;

    void putLong(int pos, long v) throws IOException;

    long getLong(int pos) throws IOException;

    void putByte(int pos, byte b) throws IOException;

    byte getByte(int pos) throws IOException;

    void putBoolean(int pos, boolean b) throws IOException;

    boolean getBoolean(int pos) throws IOException;

    void putBytes(int pos, byte[] v) throws IOException;

    byte[] getBytes(int pos, int width) throws IOException;

    void copy(int pos, int to, int width) throws IOException;
}
