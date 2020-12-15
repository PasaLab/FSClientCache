package alluxio.client.file.cache.algo.cache.lfu;

import alluxio.client.file.cache.algo.cache.*;
import alluxio.client.file.cache.algo.utils.Configuration;

public class LFUCacheFactory extends CacheFactory {

    @Override
    public AbstractCache<UnitIndex, Unit> create(Loader<UnitIndex, Unit> loader, Configuration configuration) {
        int capacity = configuration.getInt("capacity");
        boolean verbose = configuration.getBoolean("verbose");
        return new LFUCache<>(capacity, loader, verbose);
    }
}
