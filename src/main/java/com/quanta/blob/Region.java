package com.quanta.blob;

import java.io.IOException;
import java.util.Arrays;

public abstract class Region {
    protected int[] blocks;
    protected int   block_count;
    protected final int block_width;
    protected final int next_pos;
    protected final int bloom_pos;

    protected Blob blob;
    protected int header_pos;

    public Region(int block_width) {
        blocks           = new int[10];
        block_count      = 0;
        this.block_width = block_width;
        this.next_pos    = block_width;
        this.bloom_pos   = next_pos + 8;
    }

   // protected abstract long alloc() throws IOException;

  //  protected abstract boolean isFixed();

    protected abstract void create() throws IOException;

    protected void read() throws IOException {
        int next = blob.getInt(header_pos + 8);
        block_count = 0;

        while (next > 0) {
            if (block_count + 1 > blocks.length) {
                blocks = Arrays.copyOf(blocks, block_count + 10);
            }

            blocks[block_count] = next;
            next = blob.getInt(next + next_pos);
            block_count++;
        }
    }

    protected void alloc() throws IOException {
        //System.out.println("Allocating new block: current:" + block_count);
        int pos = blob.alloc(block_width + 16);

        if (block_count + 1 > blocks.length) {
            blocks = Arrays.copyOf(blocks, block_count + 10);
        }

        if (block_count == 0) {
            blob.putInt(header_pos + 8, pos);
        } else {
            //System.out.println("   Setting linked block: " + (blocks[block_count - 1] + next_pos) + "  " + pos);
            blob.putInt(blocks[block_count - 1] + next_pos, pos);
        }

        blocks[block_count++] = pos;
        //System.out.println(getClass().getSimpleName() + ": New Block " + block_count +  "> " + pos);
    }

}