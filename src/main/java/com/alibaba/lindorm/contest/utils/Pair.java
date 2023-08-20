package com.alibaba.lindorm.contest.utils;

public class Pair<K, V> {

    private final K first;

    private final V second;

    private Pair(K first, V second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Pair<?, ?> pair = (Pair<?, ?>) obj;

        return first.equals(pair.first) && second.equals(pair.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 7 + second.hashCode();
    }

    @Override
    public String toString() {
        return "{" + first + "," + second + "}";
    }

    public static <K, V> Pair<K, V> of(K k, V v) {
        return new Pair<>(k, v);
    }

    public K first() {
        return first;
    }

    public V second() {
        return second;
    }
}
