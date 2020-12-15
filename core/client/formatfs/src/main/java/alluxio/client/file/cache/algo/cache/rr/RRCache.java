package alluxio.client.file.cache.algo.cache.rr;

import alluxio.client.file.cache.algo.cache.AbstractCache;
import alluxio.client.file.cache.algo.cache.Loader;

import java.util.HashMap;
import java.util.Random;

public class RRCache<K, V> extends AbstractCache<K, V> {
    private static final int SEED = 1234;

    private HashMap<K, V> items;
    private Random random;

    public RRCache(int capacity, Loader<K, V> loader) {
        super(capacity, loader);
        items = new HashMap<>();
        random = new Random(SEED);
    }

    @Override
    public boolean isCached(K key) {
        return items.containsKey(key);
    }

    @Override
    public int size() {
        return items.size();
    }

    private void evict() {
        Object[] keys = items.keySet().toArray();
        int id = random.nextInt(keys.length);
        Object cand = keys[id];
        items.remove(cand);
    }

    @Override
    public V get(K key) {
        if (isCached(key)) {
            return items.get(key);
        } else {
            while (items.size() >= capacity) {
                evict();
            }
            V val = loader.load(key);
            items.put(key, val);
            return val;
        }
    }
}
