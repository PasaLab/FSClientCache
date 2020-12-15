package alluxio.client.file.cache.algo.cache.lfu;

import alluxio.client.file.cache.algo.cache.Loader;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class LFUCacheSuite {

    static class DummyLoader implements Loader<Integer, Integer> {

        @Override
        public Integer load(Integer key) {
            return key;
        }
    }

    @Test
    public void basic(){
        LFUCache<Integer, Integer> cache = new LFUCache<>(4, new DummyLoader());
        cache.get(1);
        cache.get(1);
        cache.get(2);
        cache.get(2);
        cache.get(3);
        cache.get(3);
        cache.get(4);
        cache.get(5);
        assertFalse(cache.isCached(4)); // evict -> 4

        cache.get(3);
        cache.get(2);
        cache.get(1);
        cache.get(5);
        cache.get(5);
        cache.get(6);
        assertFalse(cache.isCached(3)); // evict -> 3;
    }

}
