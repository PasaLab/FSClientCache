package alluxio.client.file.cache.algo.cache.arc;

import alluxio.client.file.cache.algo.cache.AbstractCache;
import alluxio.client.file.cache.algo.cache.LRUList;
import alluxio.client.file.cache.algo.cache.Loader;
import alluxio.client.file.cache.algo.utils.Pair;

import java.util.LinkedList;

public class ARCCache<K, V> extends AbstractCache<K, V> {
    private static final Object PRESENT = new Object();
    LRUList<K, V> t1;
    LRUList<K, V> t2;
    LRUList<K, Object> b1;
    LRUList<K, Object> b2;
    int p;
    boolean verbose;
    public LinkedList<K> victims;

    public ARCCache(int capacity, Loader<K, V> loader) {
        super(capacity, loader);
        p = 0;
        t1 = new LRUList<K, V>();
        t2 = new LRUList<K, V>();
        b1 = new LRUList<K, Object>();
        b2 = new LRUList<K, Object>();
        verbose = false;
        victims = new LinkedList<>();
    }

    public ARCCache(int capacity, Loader<K, V> loader, boolean verbose) {
        this(capacity, loader);
        this.verbose = verbose;
    }

    public V get(K key) {
        if (t1.contains(key)) {
            // Case I: hit
//            replace(key);
            V val = t1.removeByKey(key);
            // move from t1 to t2
            t2.pushFront(key, val);
            updateMetric(key, val, true);
            return val;
        } else if (t2.contains(key)) {
            // Case I: hit
//            replace(key);
            V val = t2.removeByKey(key);
            // move to MRU
            t2.pushFront(key, val);
            updateMetric(key, val, true);
            return val;
        } else if (b1.contains(key)) {
            // Case II:
            int delta = 1;
            if (b1.size() != 0) {
                delta = Math.max(1, b2.size() / b1.size());
            }
            updateP(p + delta);
            replace(key);
            b1.removeByKey(key);
            V val = loader.load(key);
            t2.pushFront(key, val);
            updateMetric(key, val, false);
            return val;
        } else if (b2.contains(key)) {
            // Case III:
            int delta = 1;
            if (b2.size() != 0) {
                delta = Math.max(delta, b1.size() / b2.size());
            }
            updateP(p - delta);
            replace(key);
            b2.removeByKey(key);
            V val = loader.load(key);
            t2.pushFront(key, val);
            updateMetric(key, val, false);
            return val;
        } else {
            // Cache miss both on ARC(c) and DBL(2c)
            if ((t1.size() + b1.size()) == capacity) {
                // Case A:
                if (t1.size() < capacity) {
                    b1.popBack();
                    replace(key);
                } else {
                    Pair<K, V> pair = t1.popBack();
                    victims.add(pair.getFist());
                }
            } else { // (t1.size() + b1.size()) < capacity
                int total = t1.size() + b1.size() + t2.size() + b2.size();
                if (total >= capacity) {
                    if (total >= 2 * capacity) {
                        if (b2.size() > 0) {
                            b2.popBack();
                        } else {
                            b1.popBack();
                        }
                    }
                    replace(key);
                }
            }
            V val = loader.load(key);
            t1.pushFront(key, val);
            updateMetric(key, val, false);
            return val;
        }
    }

    @Override
    public boolean isCached(K key) {
        return (t1.contains(key) || t2.contains(key));
    }

    @Override
    public int size() {
        return t1.size() + t2.size();
    }

    private void updateP(int newP) {
        int oldP = this.p;
        this.p = Math.max(0, Math.min(capacity, newP));
        if (verbose) {
            System.out.printf("UpdateP: %d -> %d\n", oldP, this.p);
            System.out.printf("t1:%d, t2:%d, b1:%d, b2:%d\n",
                    t1.size(), t2.size(), b1.size(), b2.size());
        }
    }

    private boolean isCacheFull() {
        if (t1.size() + t2.size() > capacity) {
            System.out.printf("Cache exceeds (%d > %d)\n",
                    t1.size()+t2.size(), capacity);
        }
        return (t1.size() + t2.size()) >= capacity;
    }

    private void replace(K k) {
        if (!isCacheFull()) {
            return;
        }
        if (t1.size() > 0 && (t1.size() >= p || (t1.size() == p && b2.contains(k)))) {
            Pair<K, V> pair = t1.popBack();
            victims.add(pair.getFist());
            b1.pushFront(pair.getFist(), PRESENT);
        } else {
            Pair<K, V> pair = t2.popBack();
            victims.add(pair.getFist());
            b2.pushFront(pair.getFist(), PRESENT);
        }
    }

    /* Methods for debugging & testing */

    public int getP() {
        return p;
    }

    public boolean isInT1(K k) {
        return  t1.contains(k);
    }

    public boolean isInT2(K k) {
        return t2.contains(k);
    }

    public boolean isInB1(K k) {
        return  b1.contains(k);
    }

    public boolean isInB2(K k) {
        return b2.contains(k);
    }
}
