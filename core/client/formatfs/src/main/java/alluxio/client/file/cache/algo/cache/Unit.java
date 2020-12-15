package alluxio.client.file.cache.algo.cache;

import java.nio.ByteBuffer;

public class Unit {
    public UnitIndex idx;
    public ByteBuffer buffer;

    public Unit(UnitIndex idx, ByteBuffer buffer) {
        this.idx = idx;
        this.buffer = buffer;
    }

    public Unit(long fd, long id, ByteBuffer buffer) {
        this.idx = new UnitIndex(fd, id);
        this.buffer = buffer;
    }
}
