package com.quanta.column;

import com.quanta.data.DataAdapter;
import com.quanta.data.FixedWidthDataAdapter;
import com.quanta.data.SmallIntAdapter;
import com.quanta.util.ByteBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class SortedColumn<T> extends Column<T> {

    protected FixedWidthDataAdapter<Integer> sorted;
    protected Lock lock;

    public SortedColumn(String name, DataAdapter<T> adapter, int maxUnique) {
        super(name, adapter);
        this.lock = new ReentrantLock();
        this.sorted = SmallIntAdapter.newAdapter(maxUnique);
    }

    protected void sort(T input, int index, boolean ensureUnique) throws IOException {
        if (index == 0) {
            sorted.add(index);
        } else {

            int size = sorted.size();
            int low = 0;
            int high = size - 1;

            int mid_id = 0;
            T mid_val;
            int compare = 0;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                //System.out.println(low + " : " + high + " : " + mid);
                mid_id  = sorted.get(mid);
                mid_val = adapter.get(mid_id);
                compare = adapter.compare(mid_val, input);

                if (compare < 0)
                    low = mid + 1;
                else if (compare > 0)
                    high = mid - 1;
                else if (ensureUnique) {
                    throw new IllegalStateException("Colum:" + this.name + ", Key: " + input + ", Unique key violation index: " + mid_id + ":" + mid_val + ", repeat index: " + index);
                } else {
                    break;
                }
            }

            if (compare != 0) {

                int total = size;
                int at_index = low;

                //System.out.println("Adding : [" + index + "] size: " + total + ", at: " + at_index);
                if (at_index >= total) {
                    sorted.add(index);
                } else {
                   sorted.insert(at_index, index);
                }

                //printSortedTable();
            }
        }
    }


    protected void printSortedTable() throws IOException {
        //unique + 1;
        for (int i = 0; i < sorted.size(); i++) {
            int id = sorted.get(i);
            String val = (String)adapter.get(id);

            System.out.println(id + ", " + val);
        }
        System.out.println("\n");
    }


    public int search(T value) throws IOException {
        //System.out.println("\n\nSEARCH: " + value);
        int low = 0;
        int high = adapter.size() - 1;

        int mid_id = 0;
        T mid_val = null;
        int compare = 0;

        //printSortedTable();

        while (low <= high) {
            int mid = (low + high) >>> 1;
            mid_id  = sorted.get(mid);

            mid_val = adapter.get(mid_id);
            compare = adapter.compare(mid_val, value);

            //System.out.println(low + ", " + high + ", " + mid + ", id:" + mid_id + ", v:" + mid_val + " comp:" + compare );

            if (compare < 0)
                low = mid + 1;
            else if (compare > 0)
                high = mid - 1;
            else
                return mid; // existing found
        }

        //System.out.println(" Kye NOT FOUND " + -(low + 1));
        return -(low + 1);  // key not found.
    }

    @Override
    public final ByteBitSet eq(List<T> list) throws IOException {
        lock.lock();
        try {
            ByteBitSet set = new ByteBitSet(size());

            for (T val : list) {
                int search = search(val);
                if (search > -1)
                    forIndex(set, sorted.get(search));
                    //set.set(sorted.getInt(search));
            }

            return set;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final ByteBitSet not(List<T> list) throws IOException {
        ByteBitSet set = eq(list);
        set.not();

        return set;
        /*lock.lock();
        try {
            ByteBitSet set = new ByteBitSet(adapter.size());
            //set.fill(0xFFFFFFFFFFFFFFFFl);

            for (T val : list) {
                int search = search(val);
                if (search > -1)
                    forIndex(set, sorted.get(search));
            }

            return set;
        } finally {
            lock.unlock();
        }*/
    }

    protected abstract void forIndex(ByteBitSet result, int index) throws IOException;

    @Override
    public final ByteBitSet gt(T value) throws IOException {
        lock.lock();

        try {
            int size = size();
            ByteBitSet set = new ByteBitSet(size);
            int search = Math.abs(search(value));

            for (int i = search; i < adapter.size(); i++) {
                //set.set(sorted.getInt(search));
                forIndex(set, sorted.get(i));
            }
            return set;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final ByteBitSet lt(T value) throws IOException {
        lock.lock();

        try {
            int size = size();
            ByteBitSet set = new ByteBitSet(size);
            int search = Math.abs(search(value));

            for (int i = search; i > -1; i--) {
                forIndex(set, sorted.get(i));
            }
            return set;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final ByteBitSet between(T low, T high) throws IOException {
        lock.lock();

        try {
            int size = size();
            ByteBitSet set = new ByteBitSet(size);
            int from = Math.abs(search(low));
            int to   = Math.abs(search(high));

            if (from < sorted.size()) {
                to = Math.min(to, sorted.size());

                for (int i = from; i < to; i++) {
                    forIndex(set, sorted.get(i));
                }
            }
            return set;
        } finally {
            lock.unlock();
        }
    }

    public String[] eqLabels(List<T> list) throws IOException {
        Set<String> ans = new HashSet<>();

        for (T val : list) {
            int search = search(val);
            if (search > -1)
                ans.add(adapter.toString(val));
        }

        return ans.toArray(new String[0]);
    }

    public String[] notEqLabels(List<T> list) throws IOException {
        Set<String> ans = new HashSet<>();

        Set<Integer> found = new HashSet<>();
        for (T val : list) {
            int search = search(val);
            if (search > -1)
                found.add(search);
        }

        for (int i = 0; i < adapter.size(); i++) {
            if (!found.contains(i))
                ans.add(adapter.toString(adapter.get(i)));
        }

        return ans.toArray(new String[0]);
    }

    public String[] gtLabels(T item) throws IOException {
        int search = Math.abs(search(item));
        Set<String> ans = new HashSet<>();

        for (int i = search; i < adapter.size(); i++) {
            ans.add(adapter.toString(adapter.get(i)));
        }

        return ans.toArray(new String[0]);
    }
    public String[] ltLabels(T item) throws IOException {
        Set<String> ans = new HashSet<>();

        int search = Math.abs(search(item));
        for (int i = search; i > -1; i--) {
            ans.add(adapter.toString(adapter.get(i)));
        }

        return ans.toArray(new String[0]);
    }
    public String[] betweenLabels(T low, T high) throws IOException {

        Set<String> ans = new HashSet<>();
        int from = Math.abs(search(low));
        int to   = Math.abs(search(high));

        if (from < sorted.size()) {
            to = Math.min(to, sorted.size());

            for (int i = from; i < to; i++) {
                ans.add(adapter.toString(adapter.get(i)));
            }
        }

        return ans.toArray(new String[0]);
    }
    public String[] getAllLabels() throws IOException {
        Set<String> set = new HashSet<>();
        //String[] ans = new String[adapter.size()];

        for (int i = 0; i < adapter.size(); i++) {
            set.add(adapter.toString(adapter.get(i)));
        }

        return set.toArray(new String[0]);
    }

}
