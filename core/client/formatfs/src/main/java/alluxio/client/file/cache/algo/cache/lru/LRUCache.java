package alluxio.client.file.cache.algo.cache.lru;

import alluxio.client.file.cache.algo.cache.AbstractCache;
import alluxio.client.file.cache.algo.cache.LRUList;
import alluxio.client.file.cache.algo.cache.Loader;

public class LRUCache<K, V> extends AbstractCache<K, V> {
    LRUList<K, V> lru;
    private boolean verbose;

    public LRUCache(int capacity, Loader<K, V> loader) {
        super(capacity, loader);
        lru = new LRUList<>();
        verbose = false;
    }

    public LRUCache(int capacity, Loader<K, V> loader, boolean verbose) {
        this(capacity, loader);
        this.verbose = verbose;
    }

    @Override
    public V get(K key) {
        if (lru.contains(key)) {
            V val = lru.getByKey(key);
            lru.pushFront(key, val);
            updateMetric(key, val, true);
            return val;
        } else {
            if (verbose) {
                System.out.printf("size/capacity=%d/%d\n", size(), getCapacity());
            }
            V val = loader.load(key);
            while (lru.size() >= capacity) {
                lru.popBack();
            }
            lru.pushFront(key, val);
            updateMetric(key, val, false);
            return val;
        }
    }

    @Override
    public boolean isCached(K key) {
        return lru.contains(key);
    }

    @Override
    public int size() {
        return lru.size();
    }
}
