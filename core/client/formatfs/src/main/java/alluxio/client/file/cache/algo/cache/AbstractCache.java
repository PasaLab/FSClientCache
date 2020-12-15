package alluxio.client.file.cache.algo.cache;

public abstract class AbstractCache<K, V> implements Cache<K, V> {
    protected final int capacity;
    protected final Loader<K, V> loader;

    public AbstractCache(int capacity, Loader<K, V> loader) {
        this.capacity = capacity;
        this.loader = loader;
    }

    public void updateMetric(K key, V val, boolean hit) {
    }

    public double getHitRatio() {
        return 0.0;
    }

    /**
     * Check if specific key is cached.
     * This method will not change cache status,
     * because it will no be seen as an access to cache.
     */
    public abstract boolean isCached(K key);

    public int getCapacity() {
        return capacity;
    }

    public abstract int size();
}
