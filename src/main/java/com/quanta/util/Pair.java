package com.quanta.util;

import java.util.Objects;

public class Pair<First, Second> {

    public static <A, B> Pair<A, B> create(A a, B b) {
        return new Pair<>(a, b);
    }

    public final First  first;
    public final Second second;

    public Pair(First first, Second second) {
        this.first  = first;
        this.second = second;
    }

    public First getFirst() {
        return first;
    }

    public Second getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        int hash = (first == null ? 0 : first.hashCode() * 31)
                + (second == null ? 0 : second.hashCode());

        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) {
            return false;
        }
        Pair<?, ?> p = (Pair<?, ?>) o;
        return Objects.equals(p.first, first) && Objects.equals(p.second, second);
    }
}
