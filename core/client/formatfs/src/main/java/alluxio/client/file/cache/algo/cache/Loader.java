package alluxio.client.file.cache.algo.cache;

public interface Loader<K, V> {
    /**
     * Cache uses this method to load data form source when hit miss.
     */
    public V load(K key);
}
