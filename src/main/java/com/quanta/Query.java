package com.quanta;

import com.quanta.column.Column;
import com.quanta.util.ByteBitSet;
import com.quanta.util.JSONWriter;
import com.quanta.util.Tuple;

import java.io.IOException;
import java.util.*;

/**
 * A class representing a query operation on a {@link Quanta} dataset.
 * <p>
 * The {@code Query} class provides a fluent API for filtering, selecting, and iterating over results in
 * an efficient manner using {@link ByteBitSet} for optimized filtering. It supports operations like equality,
 * inequality, greater-than, less-than, and negation on column values.
 * </p>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * Query query = quanta.newQuery()
 *     .and("region", "North America")
 *     .and("device", "Mobile", "Tablet")
 *     .gt("year", 2021)
 *     .not("month", 5, 6)
 *     .lt("day", 15);
 *
 * query.print(); // Prints filtered results
 * </pre>
 */
public class Query implements Iterable<Tuple> {

    private final Quanta quanta;
    private final ByteBitSet result;
    private boolean is_first;
    private final List<Column> selected;

    /**
     * Constructs a new {@code Query} instance for the given {@link Quanta} dataset.
     *
     * @param quanta The {@link Quanta} instance on which the query is executed.
     */
    public Query(Quanta quanta) {
        this.quanta = quanta;
        this.result = new ByteBitSet(quanta.size());
        this.is_first = true;
        this.selected = new ArrayList<>();
    }

    /**
     * Clears the current query, resetting the filter state.
     *
     * @return The cleared {@code Query} instance.
     */
    public Query clear() {
        this.is_first = true;
        this.result.clear();
        return this;
    }

    /**
     * Selects specific columns to be returned in query results.
     *
     * @param columns The names of columns to include in the results.
     */
    public void select(String... columns) {
        for (String col : columns) {
            selected.add(quanta.getColumn(col));
        }
    }

    /**
     * Filters rows based on the given column, operator, and value.
     *
     * @param column   The column name to filter on.
     * @param operator The comparison operator (e.g., "=", ">", "<").
     * @param value    The value to compare against.
     * @return The updated {@code Query} instance.
     * @throws IOException If an error occurs while filtering.
     */
    public final Query filter(String column, String operator, Object value) throws IOException {
        Column c = quanta.getColumn(column);
        ByteBitSet ans = c.filter(operator, value);

        if (is_first) {
            result.replace(ans);
            is_first = false;
        } else {
            result.and(ans);
        }

        return this;
    }

    /**
     * Filters rows where the given column matches any of the specified values (AND operation).
     *
     * @param column The column name to filter on.
     * @param values The values to match.
     * @return The updated {@code Query} instance.
     * @throws IOException If an error occurs while filtering.
     */
    public Query and(String column, Object... values) throws IOException {
        Column col = quanta.getColumn(column);
        ByteBitSet ans = col.eq(Arrays.asList(values));

        if (is_first) {
            result.replace(ans);
            is_first = false;
        } else {
            result.and(ans);
        }

        return this;
    }

    /**
     * Filters rows where the given column does not match any of the specified values (NOT operation).
     *
     * @param column The column name to filter on.
     * @param values The values to exclude.
     * @return The updated {@code Query} instance.
     * @throws IOException If an error occurs while filtering.
     */
    public Query not(String column, Object... values) throws IOException {
        Column col = quanta.getColumn(column);
        ByteBitSet ans = col.not(Arrays.asList(values));

        if (is_first) {
            result.replace(ans);
            is_first = false;
        } else {
            result.and(ans);
        }

        return this;
    }

    /**
     * Filters rows where the given column has a value greater than the specified value.
     *
     * @param column The column name to filter on.
     * @param value  The threshold value.
     * @return The updated {@code Query} instance.
     * @throws IOException If an error occurs while filtering.
     */
    public Query gt(String column, Object value) throws IOException {
        Column col = quanta.getColumn(column);
        ByteBitSet ans = col.gt(value);

        if (is_first) {
            result.replace(ans);
            is_first = false;
        } else {
            result.and(ans);
        }

        return this;
    }

    /**
     * Filters rows where the given column has a value less than the specified value.
     *
     * @param column The column name to filter on.
     * @param value  The threshold value.
     * @return The updated {@code Query} instance.
     * @throws IOException If an error occurs while filtering.
     */
    public Query lt(String column, Object value) throws IOException {
        Column col = quanta.getColumn(column);
        ByteBitSet ans = col.lt(value);

        if (is_first) {
            result.replace(ans);
            is_first = false;
        } else {
            result.and(ans);
        }

        return this;
    }

    /**
     * Returns an iterator over the filtered rows.
     *
     * @return An iterator over the query results.
     */
    @Override
    public Iterator<Tuple> iterator() {
        return new Iterator<>() {
            int i = result.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return i > -1;
            }

            @Override
            public Tuple next() {
                try {
                    Tuple row = quanta.get(i);
                    i = result.nextSetBit(i + 1);
                    return row;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Prints the query results to the console.
     */
    public void print() {
        iterator().forEachRemaining(System.out::println);
    }

    /**
     * Retrieves the selected columns for output.
     *
     * @return An array of selected {@link Column} instances.
     */
    private Column<?>[] getSelectedColumns() {
        return selected.isEmpty() ? quanta.columns() : selected.toArray(new Column[0]);
    }

    /**
     * Converts the query results into a JSON table format.
     *
     * @return A {@link JSONWriter} containing the query results in JSON format.
     * @throws IOException If an error occurs during JSON conversion.
     */
    public JSONWriter toJSONTable() throws IOException {
        JSONWriter json = new JSONWriter();
        Column<?>[] columns = getSelectedColumns();
        int col_len = columns.length;

        json.newObject();
        json.newArray("columns");
        for (Column<?> column : columns) {
            json.writeValue(column.name, true);
        }
        json.closeArray();

        int counter = 20000;

        json.newArray("rows");
        for (int rid = result.nextSetBit(0); rid > -1; rid = result.nextSetBit(rid + 1)) {
            if (counter-- <= 0) break;

            json.newArray();
            for (Column<?> column : columns) {
                column.write(json, rid);
            }
            json.closeArray();
        }
        json.closeArray();
        json.closeObject();

        return json;
    }
}
