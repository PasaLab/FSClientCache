package alluxio.client.file.cache.algo.cache;

import alluxio.client.file.cache.algo.utils.Pair;

import java.util.HashMap;
import java.util.LinkedList;

public class LRUList<K, V> {
    LinkedList<K> list;
    HashMap<K, V> items;

    public LRUList() {
        list = new LinkedList<K>();
        items = new HashMap<K, V>();
    }

    public void pushFront(K k, V v) {
        if (items.containsKey(k)) {
            list.remove(k);
        }
        items.put(k, v);
        list.addFirst(k);
    }

    public Pair<K, V> popBack() {
        if (list.size() > 0) {
            K k = list.removeLast();
            V v = items.remove(k);
            return new Pair<K, V>(k, v);
        }
        return null;
    }

    public V removeByKey(K k) {
        if (contains(k)) {
            V v = items.get(k);
            items.remove(k);
            list.remove(k);
            return v;
        }
        return null;
    }

    public boolean contains(K k) {
        return items.containsKey(k);
    }

    /**
     * Get by key without updating
     * @param k
     * @return
     */
    public V getByKey(K k) {
        if (contains(k)) {
            return items.get(k);
        }
        return null;
    }

    public int size() {
        return list.size();
    }
}
