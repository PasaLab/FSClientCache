package alluxio.client.file.cache.algo.benchmark;

import alluxio.client.file.cache.algo.cache.*;
import alluxio.client.file.cache.algo.cache.arc.ARCCacheFactory;
import alluxio.client.file.cache.algo.cache.eva.EVACacheFactory;
import alluxio.client.file.cache.algo.cache.lfu.LFUCacheFactory;
import alluxio.client.file.cache.algo.cache.lru.LRUCacheFactory;
import alluxio.client.file.cache.algo.cache.rr.RRCacheFactory;
import alluxio.client.file.cache.algo.utils.Configuration;
import alluxio.client.file.cache.algo.utils.Utils;

import java.util.HashSet;
import java.util.LinkedList;

public class Benchmark {

    public static void initCacheFactory() {
        CacheFactory.register("eva", new EVACacheFactory());
        CacheFactory.register("arc", new ARCCacheFactory());
        CacheFactory.register("lru", new LRUCacheFactory());
        CacheFactory.register("rr", new RRCacheFactory());
        CacheFactory.register("lfu", new LFUCacheFactory());
    }

    public static void main(String[] args) {
        initCacheFactory();
        Configuration configs = new Configuration(args);
        long capacity = (long) (Double.parseDouble(configs.getString("limit").replace("g", "")) * 1024 * 1024 * 1024);
        long block = configs.getLong("block");
        int numBlocks = (int) (capacity / block);
        configs.putString("capacity", Integer.toString(numBlocks));
        System.out.println(configs);
        String loaderType = configs.getString("loader");
        UnitLoader loader;
        switch (loaderType) {
            case "alluxio":
                loader = new AlluxioUnitLoader(configs.getString("filename"),
                        (int) block);
                break;
            case "mock":
            default:
                loader = new UnitLoader();
        }
        AbstractCache<UnitIndex, Unit> cache = CacheFactory.get(loader, configs);

        Metric metric = new Metric();
        System.out.println("================================");

        long readTraceStartTick = System.currentTimeMillis();
        LinkedList<TraceEntry> entries = Utils.loadTrace(configs.getString("trace"));
        long readTraceDuration = (System.currentTimeMillis() - readTraceStartTick);
        System.out.printf("trace size=%d, took %d ms\n", entries.size(), readTraceDuration);

        int repeats = configs.getInt("repeats");

        for (int i = 0; i < repeats; i++) {
            metric.reset();
            long startTick = System.currentTimeMillis();
            HashSet<Integer> unitSets = new HashSet<>();
            for (TraceEntry entry : entries) {
                long begin = entry.off;
                long end = entry.off + entry.size;
                int beginIdx = (int) (begin / block);
                int endIdx = (int) ((end - 1) / block);
                for (int idx = beginIdx; idx <= endIdx; idx++) {
                    unitSets.add(idx);
                    UnitIndex unit = new UnitIndex(0, idx);
                    long unitBegin = Math.max(begin, block * unit.id);
                    long unitEnd = Math.min(end, block * (unit.id + 1));
                    if (cache.isCached(unit)) {
                        metric.bytesHit += unitEnd - unitBegin;
                        metric.bytesRead += unitEnd - unitBegin;
                        metric.hit += 1;
                    } else {
                        metric.bytesRead += block;
                    }
                    metric.request += 1;
                    Unit unitContent = cache.get(unit);
                }

            }
            long duration = (System.currentTimeMillis() - startTick);
            System.out.printf("repeat:%d, bytesHitRatio:%f(%d/%d), duration: %d ms, total: %d ms\n",
                    i, metric.bytesHitRatio(), metric.bytesHit, metric.bytesRead, duration, readTraceDuration + duration);
            System.out.printf("hitRatio:%f(%d/%d), unique units=%d\n",
                    (double) metric.hit / (double) metric.request, metric.hit, metric.request,
                    unitSets.size());
        }
        System.out.println("================================");
    }
}
