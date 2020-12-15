package alluxio.client.file.cache.algo.cache.eva;

import alluxio.client.file.cache.algo.cache.Loader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EVACacheSuite {
    static class DummyLoader implements Loader<Integer, Integer> {

        @Override
        public Integer load(Integer key) {
            return key;
        }
    }

    @Test
    public void basic() {
        DummyLoader loader = new DummyLoader();
        EVACache<Integer, Integer> cache = new EVACache<>(4, loader, 4);
        assertEquals(new Integer(1), cache.get(1));
        assertEquals(new Integer(1), cache.get(1));
        assertEquals(new Integer(1), cache.get(1));
        assertEquals(new Integer(1), cache.get(1));
        assertEquals(1, cache.size());

        for (int i=2; i < 10; i++) {
            assertEquals(new Integer(i), cache.get(i));
            assertEquals(Math.min(4, i), cache.size());
        }
    }

    @Test
    public void high() {
        DummyLoader loader = new DummyLoader();
        EVACache<Integer, Integer> cache = new EVACache<>(4, loader, 4);
        for (int i=0; i < 1e5; i++) {
            assertEquals(new Integer(i), cache.get(i));
        }
    }
}
