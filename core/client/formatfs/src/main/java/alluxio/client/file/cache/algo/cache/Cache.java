package alluxio.client.file.cache.algo.cache;

public interface Cache<K, V> {
    /**
     * Get content of this key.
     * if it's not present, cache will load data from source.
     */
    public V get(K key);
}
