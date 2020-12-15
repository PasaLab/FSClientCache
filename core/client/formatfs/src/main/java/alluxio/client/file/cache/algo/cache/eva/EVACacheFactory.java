package alluxio.client.file.cache.algo.cache.eva;

import alluxio.client.file.cache.algo.cache.*;
import alluxio.client.file.cache.algo.utils.Configuration;

public class EVACacheFactory extends CacheFactory {
//    static {
//        CacheFactory.register("eva", new EVACacheFactory());
//    }

    @Override
    public AbstractCache<UnitIndex, Unit> create(Loader<UnitIndex, Unit> loader, Configuration configuration) {
        int capacity = configuration.getInt("capacity");
        int maxAge = configuration.getInt("maxAge");
        boolean verbose = configuration.getBoolean("verbose");
        double ewmaDecay = configuration.getDouble("ewmaDecay");
        return new EVACache<UnitIndex, Unit>(capacity, loader, maxAge, ewmaDecay, verbose);
    }
}
