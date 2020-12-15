package alluxio.client.file.cache.algo.benchmark;

import java.util.Objects;

public class TraceEntry {
    public long off;
    public int size;

    public TraceEntry(long off, int size) {
        this.off = off;
        this.size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraceEntry entry = (TraceEntry) o;
        return off == entry.off;
    }

    @Override
    public int hashCode() {
        return Objects.hash(off, size);
    }
}
