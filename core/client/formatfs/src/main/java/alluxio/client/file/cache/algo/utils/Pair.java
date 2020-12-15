package alluxio.client.file.cache.algo.utils;

import java.util.Objects;

public class Pair<K, V> {
    K fist;
    V second;

    public Pair(K k, V v) {
        this.fist = k;
        this.second = v;
    }

    public K getFist() {
        return fist;
    }

    public V getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(fist, pair.fist) &&
                Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fist, second);
    }
}
