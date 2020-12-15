package alluxio.client.file.cache.algo.benchmark;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.algo.cache.Unit;
import alluxio.client.file.cache.algo.cache.UnitIndex;
import alluxio.client.file.cache.algo.cache.UnitLoader;

import java.nio.ByteBuffer;

public class AlluxioUnitLoader extends UnitLoader {
    String fn;
    FileSystem fs;
    AlluxioURI path;
    FileInStream in;
    int UNIT_SIZE;

    public AlluxioUnitLoader(String fn, int unitSize) {
        this.fn = fn;
        UNIT_SIZE = unitSize;
        fs = CacheFileSystem.get(false);
        path = new AlluxioURI(fn);
        try {
            in = fs.openFile(path);
        } catch (Exception e) {
            System.out.println("Cannot open file " + path );
            System.exit(1);
        }
    }

    @Override
    public Unit load(UnitIndex key) {
        byte[] buffer = new byte[UNIT_SIZE];
        int nread = 0;
        long off = key.id * UNIT_SIZE;
        try {
            nread = in.positionedRead(off, buffer, 0, UNIT_SIZE);
            if (nread != UNIT_SIZE) {
                System.out.printf("load error (%d != %d)\n", nread, UNIT_SIZE);
            }
        } catch (Exception e) {
            System.out.printf("Failed to load(off=%d, size=%d)\n", off, UNIT_SIZE);
            e.printStackTrace();
        }
        return new Unit(key, ByteBuffer.wrap(buffer));
    }
}
