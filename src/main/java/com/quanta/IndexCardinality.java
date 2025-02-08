package com.quanta;

/**
 * Enum representing different levels of index cardinality.
 * <p>
 * Index cardinality determines the maximum number of distinct values
 * a column can store while still benefiting from optimized indexing.
 * </p>
 *
 * <p>Usage Example:</p>
 * <pre>
 * Quanta quanta = new QuantaBuilder("Dataset", "/data")
 *     .addStringColumn("symbol", 10, IndexCardinality.TINY)
 *     .addIntColumn("year", IndexCardinality.MEDIUM)
 *     .getQuanta();
 * </pre>
 *
 * <p>
 * Choosing the right index cardinality is crucial for performance and memory usage.
 * </p>
 */
public enum IndexCardinality {

    /** Supports up to 255 unique values. Ideal for low-cardinality fields like booleans or enums. */
    TINY(255),

    /** Supports up to 65,534 unique values. Suitable for categorical data with medium variability. */
    SMALL(65_534),

    /** Supports up to 16,777,214 unique values. Used for high-cardinality fields. */
    MEDIUM(16_777_214),

    /** Supports up to 16,777,215 unique values. Recommended for primary keys or highly unique data. */
    LARGE(16_777_215);

    private final int maxDistinct;

    /**
     * Constructs an {@code IndexCardinality} enum with the specified maximum distinct values.
     *
     * @param maxDistinct The maximum number of unique values the index can support.
     */
    IndexCardinality(int maxDistinct) {
        this.maxDistinct = maxDistinct;
    }

    /**
     * Returns the maximum number of distinct values allowed for this index cardinality level.
     *
     * @return The maximum distinct values supported.
     */
    public int getMaxDistinct() {
        return maxDistinct;
    }
}
