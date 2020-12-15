package alluxio.client.file.cache.algo.benchmark;

import alluxio.AlluxioURI;
import alluxio.client.file.CacheFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.algo.cache.Loader;

public class AlluxioLoader implements Loader<TraceEntry, Content> {
    String fn;
    FileSystem fs;
    AlluxioURI path;
    FileInStream in;
    int UNIT_SIZE;

    public AlluxioLoader(String fn, int unitSize) {
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
    public Content load(TraceEntry key) {
        byte[] buffer = new byte[UNIT_SIZE];
        int nread = 0;
        try {
            nread = in.positionedRead(key.off, buffer, 0, UNIT_SIZE);
        } catch (Exception e) {
            System.out.printf("Failed to load(off=%d, size=%d)\n", key.off, UNIT_SIZE);
            e.printStackTrace();
        }
        return new Content(buffer);
    }
}
