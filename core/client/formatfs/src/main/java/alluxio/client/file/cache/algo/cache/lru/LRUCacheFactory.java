package alluxio.client.file.cache.algo.cache.lru;

import alluxio.client.file.cache.algo.cache.*;
import alluxio.client.file.cache.algo.utils.Configuration;

public class LRUCacheFactory extends CacheFactory {
    @Override
    public AbstractCache<UnitIndex, Unit> create(Loader<UnitIndex, Unit> loader, Configuration configuration) {
        int capacity = configuration.getInt("capacity");
        boolean verbose = configuration.getBoolean("verbose");
        return new LRUCache<>(capacity, loader, verbose);
    }
}
