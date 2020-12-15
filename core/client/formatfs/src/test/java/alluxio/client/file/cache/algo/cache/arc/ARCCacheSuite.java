package alluxio.client.file.cache.algo.cache.arc;

import alluxio.client.file.cache.algo.cache.Loader;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.*;

public class ARCCacheSuite {

    static class Tuple3 {
        int fd;
        int off;
        int size;

        Tuple3(int fd, int off, int size) {
            this.fd = fd;
            this.off = off;
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple3 tuple3 = (Tuple3) o;
            return fd == tuple3.fd &&
                    off == tuple3.off &&
                    size == tuple3.size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fd, off, size);
        }
    }

    static class Content {
        byte[] buffer;

        Content(byte[] buffer) {
            this.buffer = buffer;
        }
    }

    static class DummyLoader implements Loader<Tuple3, Content> {

        @Override
        public Content load(Tuple3 key) {
            byte[] buffer = new byte[key.size];
            return new Content(buffer);
        }
    }

    static class DummyCache extends ARCCache<Tuple3, Content> {
        long bytesRead;
        long bytesHit;

        DummyCache(int capacity) {
            super(capacity, new DummyLoader());
            bytesRead = 0;
            bytesHit = 0;
        }

        @Override
        public double getHitRatio() {
            return (double) bytesHit / (double) bytesRead;
        }

        @Override
        public void updateMetric(Tuple3 key, Content value, boolean hit) {
            if (hit) {
                bytesHit += key.size;
            }
            bytesRead += key.size;
        }
    }

    @Test
    public void base() {
        DummyCache cache = new DummyCache(4);
        Tuple3 x1 = new Tuple3(0, 0, 4);
        Content cont = cache.get(x1);
    }

    @Test
    public void getTest() {
        DummyCache cache = new DummyCache(4);
        for (int i=0; i < 4; i++) {
            Tuple3 x = new Tuple3(0, i*4, 4);
            Content cont = cache.get(x);
        }
        assertEquals(0.0, (double) cache.getHitRatio(), 0.000001);

        for (int i=0; i < 4; i++) {
            Tuple3 x = new Tuple3(0, i*4, 4);
            Content cont = cache.get(x);
        }
        assertEquals(0.5, (double) cache.getHitRatio(), 0.000001);
    }

    @Test
    public void replaceTest() {
        //
        DummyCache cache = new DummyCache(4);
        Tuple3 a = new Tuple3(0, 1, 1);
        Tuple3 b = new Tuple3(0, 2, 1);
        Tuple3 c = new Tuple3(0, 3, 1);
        Tuple3 d = new Tuple3(0, 4, 1);
        Tuple3 e = new Tuple3(0, 5, 1);
        Tuple3 f = new Tuple3(0, 6, 1);
        cache.get(a);
        cache.get(b);
        cache.get(c);
        cache.get(d);
        assertEquals(4, cache.size());
        assertEquals(0, cache.getP());
        assertTrue(cache.isInT1(a));
        assertTrue(cache.isInT1(b));
        assertTrue(cache.isInT1(c));
        assertTrue(cache.isInT1(d));

        cache.get(a);
        assertTrue(cache.isInT2(a));

        cache.get(e);
        assertEquals(4, cache.size());
        // b is evicted
        assertFalse(cache.isCached(b));

        cache.get(b);
        // c is evicted
        assertFalse(cache.isCached(c));
        // b is moved to t2;
        assertTrue(cache.isInT2(b));
        // and p increase 1 because b is hit on b1
        assertEquals(1, cache.getP());
        assertTrue(cache.isInB1(c));

        assertTrue(cache.isInB1(c));
        cache.get(c); // c -> T2
        assertEquals(2, cache.getP());
        assertFalse(cache.isCached(d)); // d -> b1

        // now: p = 2
        // t1: e        b1: d
        // t2: a b c    b2:

        cache.get(f);
        // a -> b2; f -> t1;
        assertTrue(cache.isInB2(a));
        assertTrue(cache.isInT1(f));

        // now: p = 2
        // t1: e f       b1: d
        // t2: b c      b2: a

        cache.get(d);
        // d -> t2; b -> b2; p->3;
        assertTrue(cache.isInT2(d));
        assertTrue(cache.isInB2(b));
        assertEquals(3, cache.getP());

        // now: p = 3
        // t1: e f       b1: d
        // t2: c d      b2: a b

        cache.get(a);
        // a -> t2; e -> b2; p -> 2;
        assertTrue(cache.isInT2(a));
        assertTrue(cache.isInB1(e));
        assertEquals(2, cache.getP());

    }
}
