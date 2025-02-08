package com.quanta.util;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * A lightweight data structure for storing and manipulating key-value pairs.
 * <p>
 * {@code Tuple} provides a flexible way to handle row-based data, allowing for dynamic key-value storage,
 * efficient data retrieval, merging, copying, and conversion to JSON.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * Tuple tuple = new Tuple();
 * tuple.set("id", 123)
 *      .set("name", "Alice")
 *      .set("age", 25);
 *
 * int id = tuple.get("id"); // Retrieves 123
 * String name = tuple.get("name", "Unknown"); // Retrieves "Alice"
 *
 * tuple.remove("age"); // Removes "age" key
 *
 * JSONObject json = tuple.toJson(); // Converts tuple to JSON
 * </pre>
 */
public class Tuple {

    /**
     * Internal storage for key-value pairs.
     */
    public final Map<String, Object> data;

    /**
     * Constructs an empty {@code Tuple}.
     */
    public Tuple() {
        this.data = new LinkedHashMap<>();
    }

    /**
     * Constructs a {@code Tuple} from an existing map.
     *
     * @param row The map containing key-value pairs to initialize the tuple.
     */
    public Tuple(Map<String, Object> row) {
        this.data = new HashMap<>(row);
    }

    /**
     * Retrieves the value associated with the given key.
     *
     * @param col The key whose value is to be retrieved.
     * @param <T> The expected type of the value.
     * @return The value associated with the key, or {@code null} if the key does not exist.
     */
    public <T> T get(String col) {
        return (T) data.get(col);
    }

    /**
     * Retrieves the value associated with the given key, returning a default value if the key is not present.
     *
     * @param col The key whose value is to be retrieved.
     * @param default_value The default value to return if the key is not found.
     * @param <T> The expected type of the value.
     * @return The value associated with the key, or {@code default_value} if the key does not exist.
     */
    public <T> T get(String col, T default_value) {
        T value = (T) data.get(col);
        return value == null ? default_value : value;
    }

    /**
     * Adds or updates a key-value pair in the tuple.
     *
     * @param col The key.
     * @param v The value to be associated with the key.
     * @return The updated {@code Tuple} instance.
     */
    public Tuple set(String col, Object v) {
        this.data.put(col, v);
        return this;
    }

    /**
     * Removes multiple keys from the tuple.
     *
     * @param cols The keys to be removed.
     */
    public void removeMany(String... cols) {
        for (String col : cols)
            data.remove(col);
    }

    /**
     * Removes a key-value pair from the tuple.
     *
     * @param col The key to remove.
     * @param <T> The expected type of the removed value.
     * @return The value associated with the removed key, or {@code null} if the key was not present.
     */
    public <T> T remove(String col) {
        return (T) data.remove(col);
    }

    /**
     * Merges another tuple's key-value pairs into this tuple.
     * <p>
     * If the same key exists in both tuples, the value from the other tuple will override the existing value.
     * </p>
     *
     * @param other The tuple whose data should be merged into this tuple.
     */
    public void merge(Tuple other) {
        this.data.putAll(other.data);
    }

    /**
     * Checks whether the tuple contains a specific key.
     *
     * @param col The key to check.
     * @return {@code true} if the key exists, otherwise {@code false}.
     */
    public boolean has(String col) {
        return data.containsKey(col);
    }

    /**
     * Clears all key-value pairs from the tuple.
     */
    public void clear() {
        this.data.clear();
    }

    /**
     * Returns the number of key-value pairs in the tuple.
     *
     * @return The number of stored key-value pairs.
     */
    public int size() {
        return data.size();
    }

    /**
     * Checks if the tuple is empty.
     *
     * @return {@code true} if the tuple contains no key-value pairs, otherwise {@code false}.
     */
    public boolean isEmpty() {
        return data.size() == 0;
    }

    /**
     * Returns a string representation of the tuple.
     *
     * @return A string representation of the tuple's key-value pairs.
     */
    @Override
    public String toString() {
        return data.toString();
    }

    /**
     * Checks if this tuple is equal to another object.
     *
     * @param o The object to compare with.
     * @return {@code true} if the tuples contain the same key-value pairs, otherwise {@code false}.
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        return this.data.equals(((Tuple) o).data);
    }

    /**
     * Returns the hash code of the tuple.
     *
     * @return The hash code of the tuple.
     */
    @Override
    public int hashCode() {
        return data.hashCode();
    }

    /**
     * Iterates over each key-value pair in the tuple and performs the given action.
     *
     * @param action A {@link BiConsumer} that consumes each key-value pair.
     */
    public void forEach(BiConsumer<String, Object> action) {
        data.forEach(action);
    }

    /**
     * Creates a deep copy of this tuple.
     *
     * @return A new {@code Tuple} instance with the same key-value pairs.
     */
    public Tuple copy() {
        return new Tuple(new HashMap<>(data));
    }

    /**
     * Copies all key-value pairs from this tuple to another tuple.
     *
     * @param t The target tuple.
     */
    public void copyTo(Tuple t) {
        data.forEach(t::set);
    }

    /**
     * Copies only the specified keys from this tuple to another tuple.
     *
     * @param t    The target tuple.
     * @param cols The specific keys to copy.
     */
    public void copy(Tuple t, String... cols) {
        for (String c : cols) {
            t.set(c, data.get(c));
        }
    }

    /**
     * Returns a set of all keys in the tuple.
     *
     * @return A {@link Set} containing all keys in the tuple.
     */
    public Set<String> keys() {
        return data.keySet();
    }
}
