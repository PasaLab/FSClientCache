package alluxio.client.file.cache.algo.cache;

import alluxio.client.file.cache.algo.utils.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

public class LRUListSuite {

    @Test
    public void base() {
        LRUList<Integer, String> lruList = new LRUList<>();
        lruList.pushFront(0, "zero");
        assertTrue(lruList.contains(0));

        Pair<Integer, String> pair = lruList.popBack();
        assertEquals(0, (int) pair.getFist());
        assertEquals("zero", pair.getSecond());

        pair = lruList.popBack();
        assertNull(pair);

        lruList.pushFront(0, "zero");
        lruList.pushFront(1, "one");
        lruList.pushFront(2, "two");
        lruList.pushFront(3, "three");
        pair = lruList.popBack();
        assertEquals(0, (int) pair.getFist());
        assertEquals("zero", pair.getSecond());

        lruList.pushFront(1, "one");
        assertEquals(3, lruList.size());
        pair = lruList.popBack();
        assertEquals(2, (int) pair.getFist());
        assertEquals("two", pair.getSecond());

        lruList.removeByKey(1);
        assertEquals(1, lruList.size());
        assertFalse(lruList.contains(1));
    }

}
