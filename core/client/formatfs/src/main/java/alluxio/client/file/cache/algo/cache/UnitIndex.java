package alluxio.client.file.cache.algo.cache;

public class UnitIndex {
    public long fd;
    public long id;

    public UnitIndex(long fd, long id) {
        this.fd = fd;
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnitIndex unitIndex = (UnitIndex) o;
        return fd == unitIndex.fd &&
                id == unitIndex.id;
    }

    @Override
    public int hashCode() {
//        return Objects.hash(fd, id);
        return (int)(fd * 31 + id) * 31;
    }

    @Override
    public String toString() {
        return "UnitIndex{" +
                "fd=" + fd +
                ", id=" + id +
                '}';
    }
}
