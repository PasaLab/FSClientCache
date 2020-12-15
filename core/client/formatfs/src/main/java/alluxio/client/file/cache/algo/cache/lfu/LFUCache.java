package alluxio.client.file.cache.algo.cache.lfu;

import alluxio.client.file.cache.algo.cache.AbstractCache;
import alluxio.client.file.cache.algo.cache.Loader;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Objects;
import java.util.PriorityQueue;

public class LFUCache<K, V> extends AbstractCache<K, V> {
    private HashMap<K, V> items;
    private HashMap<K, KeyWithPriority<K>> indexs;
    private PriorityQueue<KeyWithPriority<K>> pq;
    private boolean verbose;
    private int now;

    static class KeyWithPriority<K> {
        K key;
        int hits;
        int timestamp;

        public KeyWithPriority(K key, int hits, int timestamp) {
            this.key = key;
            this.hits = hits;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyWithPriority<?> that = (KeyWithPriority<?>) o;
            return hits == that.hits &&
                    timestamp == that.timestamp &&
                    Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, hits, timestamp);
        }

        public boolean lessThan(KeyWithPriority<K> other) {
            if (this.hits == other.hits) {
                return this.timestamp < other.timestamp;
            }
            return this.hits < other.hits;
        }
    }

    public LFUCache(int capacity, Loader<K, V> loader) {
        super(capacity, loader);
        verbose = false;
        items = new HashMap<>();
        indexs = new HashMap<>();
        now = 0;
        Comparator<KeyWithPriority<K>> comparator = new Comparator<KeyWithPriority<K>>() {
            @Override
            public int compare(KeyWithPriority<K> o1, KeyWithPriority<K> o2) {
                if (o1.hits == o2.hits) {
                    return o1.timestamp - o2.timestamp;
                }
                return o1.hits - o2.hits;
            }
        };
        pq = new PriorityQueue<>(comparator);
    }

    public LFUCache(int capacity, Loader<K, V> loader, boolean verbose) {
        this(capacity, loader);
        this.verbose = verbose;
    }

    @Override
    public boolean isCached(K key) {
        return items.containsKey(key);
    }

    @Override
    public int size() {
        return items.size();
    }

    @Override
    public V get(K key) {
        if (isCached(key)) {
            KeyWithPriority<K> kp = indexs.get(key);
            pq.remove(kp);
            kp.hits ++;
            kp.timestamp = now++;
            pq.offer(kp);
            return items.get(key);
        } else {
            if (verbose) {
                System.out.printf("size/capacity=%d/%d\n", size(), capacity);
            }
            while (size() >= capacity) {
                evict();
            }
            V val = loader.load(key);
            items.put(key, val);
            KeyWithPriority<K> kp = new KeyWithPriority<>(key, 0, now++);
            indexs.put(key, kp);
            pq.offer(kp);
            return val;
        }
    }

    private void evict() {
        KeyWithPriority<K> kp = pq.poll();
        if (kp == null) {
            System.out.println("Candidate should not be null");
        } else{
            items.remove(kp.key);
            indexs.remove(kp.key);
        }
    }
}
