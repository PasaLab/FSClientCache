package alluxio.client.file.cache.core;

import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;

public class MockFileInStreamWithCache extends FileInStreamWithCache {
    public MockFileInStreamWithCache(InStreamOptions opt, ClientCacheContext context, URIStatus status) {
        super(opt, context, status);
    }

    @Override
    public int innerRead(byte[] b, int off, int len) {
        return len;
    }

    @Override
    public int innerPositionRead(long pos, byte[] b, int off, int len) {
        return len;
    }

    @Override
    public void close() {
        // nothing to to
    }
}
