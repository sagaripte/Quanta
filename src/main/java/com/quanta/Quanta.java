package com.quanta;

import com.quanta.column.IndexedColumn;
import com.quanta.column.Column;
import com.quanta.util.JSONWriter;
import com.quanta.util.Tuple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Core class representing an in-memory database for high-performance analytical queries.
 * <p>
 * {@code Quanta} provides low-level access to data storage, indexing, and retrieval but is not
 * intended to be used directly by application developers.
 * </p>
 * <p><b>💡 Recommended Usage:</b></p>
 * - Use {@link QuantaBuilder} to define and build a database schema.
 * - Use {@link Query} to filter and retrieve data efficiently.
 *
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * // Recommended approach: Using QuantaBuilder and Query
 * Quanta quanta = new QuantaBuilder("Dataset", "/data/storage")
 *     .addPrimaryKeyIntColumn("id")
 *     .addDictionaryColumn("category", 10)
 *     .addFact("price")
 *     .getQuanta();
 *
 * Query q = quanta.newQuery()
 *     .and("category", "Electronics")
 *     .gt("price", 100);
 *
 * q.iterator();
 * </pre>
 */
public class Quanta implements Closeable {

    /** The name of the Quanta dataset. */
    public final String name;

    /** The base directory where data is stored. */
    public final String base_dir;

    /** The total number of records in the dataset. */
    private int size;

    /** A map of column names to their respective {@link Column} instances. */
    private final Map<String, Column<?>> columns;

    /**
     * Constructs a new {@code Quanta} instance for the given name and storage location.
     * <p>💡 Use {@link QuantaBuilder} to create a Quanta instance instead of calling this constructor directly.</p>
     *
     * @param name     The name of the dataset.
     * @param base_dir The directory where data files are stored.
     */
    public Quanta(String name, String base_dir) {
        this.name = name;
        this.base_dir = base_dir;
        this.size = 0;
        this.columns = new LinkedHashMap<>();
        new File(base_dir).mkdirs();
    }

    /**
     * Creates a new {@link Query} instance for filtering and retrieving data.
     *
     * @return A new {@link Query} object associated with this Quanta dataset.
     */
    public Query newQuery() {
        return new Query(this);
    }

    /**
     * Returns the number of records in the dataset.
     *
     * @return The total number of records.
     */
    public int size() {
        return size;
    }

    /**
     * Rebuilds all indexed columns to optimize query performance.
     * <p>
     * This method must be called after new data is ingested for rebuilding indexes.
     * </p>
     *
     * @throws IOException If an error occurs during index rebuilding.
     */
    public void rebuild() throws IOException {
        for (Column<?> c : columns.values()) {
            if (c instanceof IndexedColumn) {
                try {
                    ((IndexedColumn<?>) c).rebuild();
                } catch (RuntimeException e) {
                    throw e;
                }
            }
        }
    }

    /**
     * Closes all column resources, ensuring that file-backed storage is properly flushed.
     *
     * @throws IOException If an error occurs while closing resources.
     */
    @Override
    public void close() throws IOException {
        for (Column<?> c : columns.values()) {
            c.close();
        }
    }

    /**
     * Adds a new column to the dataset.
     * <p>💡 This method should not be used directly; use {@link QuantaBuilder} instead.</p>
     *
     * @param name   The name of the column.
     * @param column The {@link Column} instance to add.
     */
    public void addColumn(String name, Column<?> column) {
        columns.put(name, column);
        this.size = column.size();
    }

    /**
     * Retrieves a column by name.
     *
     * @param name The column name.
     * @return The {@link Column} instance.
     * @throws NullPointerException If the column does not exist.
     */
    protected Column<?> getColumn(String name) {
        if (!columns.containsKey(name)) {
            throw new NullPointerException("No column named " + name);
        }
        return columns.get(name);
    }

    /**
     * Checks if the specified column is a fact column (numerical measure).
     *
     * @param name The column name.
     * @return {@code true} if the column is a fact column, otherwise {@code false}.
     */
    public boolean isFact(String name) {
        return getColumn(name).getColumnType() == 5;
    }

    /**
     * Adds a new row to the dataset.
     * <p>
     * Each row must contain values for all defined columns.
     * </p>
     *
     * @param row The {@link Tuple} containing key-value pairs for column data.
     * @throws RuntimeException If data insertion fails.
     */
    public void add(Tuple row) {
        Exception ex = null;

        try {
            for (Column<?> tc : columns.values()) {
                if (row.has(tc.name)) {
                    try {
                        tc.add(row.get(tc.name));
                    } catch (Exception e2) {
                        ex = e2;
                    }
                } else {
                    throw new IllegalArgumentException("Column '" + tc.name + "' not found in row");
                }
            }
            size++;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (ex != null) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Returns an array of all columns in the dataset.
     *
     * @return An array of {@link Column} objects.
     */
    public Column<?>[] columns() {
        return columns.values().toArray(new Column[0]);
    }

    /**
     * Retrieves a row from the dataset by index.
     *
     * @param index The row index.
     * @return A {@link Tuple} containing column values for the row.
     * @throws IOException If an error occurs while fetching data.
     */
    public Tuple get(int index) throws IOException {
        Tuple row = new Tuple();
        for (Map.Entry<String, Column<?>> col : columns.entrySet()) {
            row.set(col.getKey(), col.getValue().get(index));
        }
        return row;
    }

    /**
     * Retrieves dataset metadata, including column definitions, in JSON format.
     *
     * @return A byte array representing the dataset metadata in JSON.
     * @throws IOException If an error occurs during metadata retrieval.
     */
    public byte[] getMeta() throws IOException {
        JSONWriter jcols = new JSONWriter();
        for (Column<?> col : columns.values()) {
            col.writeMeta(jcols);
        }

        JSONWriter meta = new JSONWriter();
        meta.newObject();
        meta.newArray("columns");
        meta.writeValue(jcols.toByteArray(), false);
        meta.closeArray();
        meta.closeObject();

        return meta.toByteArray();
    }
}
