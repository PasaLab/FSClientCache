package alluxio.client.file.cache.algo.cache;

import alluxio.client.file.cache.algo.utils.Configuration;

import java.util.HashMap;

public abstract class CacheFactory {
    private static final HashMap<String, CacheFactory> factories = new HashMap<>();

    public static AbstractCache<UnitIndex, Unit> get(Loader<UnitIndex, Unit> loader, Configuration configuration) {
        String policy = configuration.getString("policy");
        return factories.get(policy).create(loader, configuration);
    }

    public static void register(String name, CacheFactory factory) {
        factories.put(name, factory);
    }

    abstract public AbstractCache<UnitIndex, Unit> create(Loader<UnitIndex, Unit> loader, Configuration configuration);
}
