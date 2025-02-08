package com.quanta;

import com.quanta.column.*;
import com.quanta.data.*;
import com.quanta.util.Utils;

import java.io.File;
import java.io.IOException;

/**
 * Builder class for constructing a {@link Quanta} instance with various column types and indexing strategies.
 * <p>
 * QuantaBuilder provides a fluent API for defining the schema of a Quanta database, allowing users to specify
 * different types of columns, indexing mechanisms, and data persistence configurations.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * Quanta quanta = new QuantaBuilder("AdEvents", "/data/ad_tracking", true)
 *     .addPrimaryKeyIntColumn("event_id")
 *     .addStringColumn("campaign", 30, IndexCardinality.TINY)
 *     .addDictionaryColumn("region", 10)
 *     .addIntColumn("year", IndexCardinality.TINY)
 *     .addFact("bid_price")
 *     .getQuanta();
 * </pre>
 */
public class QuantaBuilder {

    /**
     * Internal enum defining the different indexing strategies available.
     */
    private enum IndexType {
        UNIQUE_VALUES, INDEXED, NO_INDEX
    }

    private Quanta quanta;

    /**
     * Constructs a new {@code QuantaBuilder} instance with the specified name and storage location.
     * If {@code cleanup} is set to {@code true}, it deletes any existing data in the specified location.
     *
     * @param name     The name of the Quanta dataset.
     * @param location The directory where Quanta data will be stored.
     * @param cleanup  If {@code true}, existing data at the location is deleted before initializing.
     */
    public QuantaBuilder(String name, String location, boolean cleanup) {
        if (cleanup)
            Utils.deleteDir(new File(location));
        this.quanta = new Quanta(name, location);
    }

    /**
     * Constructs a new {@code QuantaBuilder} instance with the specified name and storage location.
     * This constructor does not delete any existing data.
     *
     * @param name     The name of the Quanta dataset.
     * @param location The directory where Quanta data will be stored.
     */
    public QuantaBuilder(String name, String location) {
        this.quanta = new Quanta(name, location);
    }

    /**
     * Adds a dictionary-encoded column for storing string values efficiently.
     *
     * @param name     The column name.
     * @param maxWidth The maximum possible width of the string in this column.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addDictionaryColumn(String name, int maxWidth) throws IOException {
        return addColumn(name, new Dictionary(maxWidth), IndexType.INDEXED, IndexCardinality.TINY.getMaxDistinct());
    }

    /**
     * Adds a string column with optional indexing based on cardinality.
     *
     * @param name        The column name.
     * @param maxWidth    The maximum possible width of the string in this column.
     * @param cardinality The expected number of distinct values for indexing.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addStringColumn(String name, int maxWidth, IndexCardinality cardinality) throws IOException {
        DataAdapter da = cardinality == IndexCardinality.TINY ? new Dictionary(maxWidth) : new FixedStringAdapter(maxWidth);

        return addColumn(name, da, IndexType.INDEXED, cardinality.getMaxDistinct());
    }

    /**
     * Adds a primary key integer column where each int is going to be unique.
     *
     * @param name The column name.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addPrimaryKeyIntColumn(String name) throws IOException {
        return addColumn(name, new IntAdapter(), IndexType.UNIQUE_VALUES, IndexCardinality.LARGE.getMaxDistinct());
    }

    /**
     * Adds an indexed integer column.
     *
     * @param name        The column name.
     * @param cardinality The expected number of distinct values for indexing.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addIntColumn(String name, IndexCardinality cardinality) throws IOException {
        return addColumn(name, new IntAdapter(), IndexType.INDEXED, cardinality.getMaxDistinct());
    }

    /**
     * Adds a timestamp column with unique indexing.
     *
     * It's recommended to split date in year, month, day columns for faster storage and queries
     *
     * @param name   The column name.
     * @param format The timestamp format.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addUniqueDatesColumn(String name, String format) throws IOException {
        return addColumn(name, new TimestampAdapter(format), IndexType.UNIQUE_VALUES, IndexCardinality.LARGE.getMaxDistinct());
    }

    /**
     * Adds an indexed timestamp column.
     *
     * It's recommended to split date in year, month, day columns for faster storage and queries
     *
     * @param name        The column name.
     * @param format      The timestamp format.
     * @param cardinality The expected number of distinct values for indexing.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addTimestampColumn(String name, String format, IndexCardinality cardinality) throws IOException {
        return addColumn(name, new TimestampAdapter(format), IndexType.INDEXED, cardinality.getMaxDistinct());
    }

    public QuantaBuilder addTimestampColumn(String name, IndexCardinality cardinality) throws IOException {
        return addTimestampColumn(name, "yyyy-MM-dd", cardinality);
    }

    /**
     * Adds a boolean column.
     *
     * @param name The column name.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addBooleanColumn(String name) throws IOException {
        return addColumn(name, new BooleanAdapter(), IndexType.INDEXED, 2);
    }

    /**
     * Adds a floating-point fact column (e.g., prices, metrics).
     *
     * @param name The column name.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addFact(String name) throws IOException {
        String file  = quanta.base_dir + "/" + name;
        quanta.addColumn(name, new FactColumn(name, file, new DoubleAdapter()));

        return this;
    }

    /**
     * Adds an integer fact column.
     *
     * @param name The column name.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    public QuantaBuilder addIntegerFact(String name) throws IOException {
        String file  = quanta.base_dir + "/" + name;
        quanta.addColumn(name, new FactColumn(name, file, new IntAdapter()));

        return this;
    }

    /**
     * Builds and returns the {@link Quanta} instance configured by this builder.
     *
     * @return The constructed {@link Quanta} instance.
     */
    public Quanta getQuanta() {
        return quanta;
    }

    /**
     * Internal method to add a column to the Quanta dataset.
     *
     * @param name       The column name.
     * @param da         The data adapter to use.
     * @param indexType  The indexing type to apply.
     * @param maxUnique  Maximum number of unique values for indexed columns.
     * @return The updated {@code QuantaBuilder} instance.
     * @throws IOException If an error occurs while adding the column.
     */
    private QuantaBuilder addColumn(String name, DataAdapter da, IndexType indexType, int maxUnique) throws IOException {

        Column col;
        String file  = quanta.base_dir + "/" + name;

        switch (indexType) {
            case INDEXED -> col = new IndexedColumn(name, file, da, maxUnique);
            case UNIQUE_VALUES -> col = new AllUniqueValuesIndexColumn(name, file, da);
            case NO_INDEX -> col = new NoIndexColumn(name, file, da);
            default -> throw new IllegalArgumentException("No Index type provided");
        }
        quanta.addColumn(name, col);

        return this;
    }
}
