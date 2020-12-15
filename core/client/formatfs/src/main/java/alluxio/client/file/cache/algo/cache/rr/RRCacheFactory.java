package alluxio.client.file.cache.algo.cache.rr;

import alluxio.client.file.cache.algo.cache.*;
import alluxio.client.file.cache.algo.utils.Configuration;

public class RRCacheFactory extends CacheFactory {
    @Override
    public AbstractCache<UnitIndex, Unit> create(Loader<UnitIndex, Unit> loader, Configuration configuration) {
        int capacity = configuration.getInt("capacity");
        return new RRCache<>(capacity, loader);
    }
}
