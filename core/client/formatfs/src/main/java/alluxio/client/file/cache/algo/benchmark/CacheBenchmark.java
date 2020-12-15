package alluxio.client.file.cache.algo.benchmark;

import alluxio.client.file.cache.algo.cache.AbstractCache;
import alluxio.client.file.cache.algo.cache.Metric;
import alluxio.client.file.cache.algo.cache.Unit;
import alluxio.client.file.cache.algo.cache.UnitIndex;
import alluxio.client.file.cache.algo.cache.arc.ARCCache;
import alluxio.client.file.cache.algo.cache.eva.EVACache;
import alluxio.client.file.cache.algo.cache.lru.LRUCache;
import alluxio.client.file.cache.algo.utils.Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;

public class CacheBenchmark {
    String policy;
    String trace;
    String path;
    long capacity;
    int block;
    int numBlocks;
    AlluxioUnitLoader loader;

    AbstractCache<UnitIndex, Unit> cache;

    public CacheBenchmark(String type, String trace, String path, long capacity, int block) {
        this.policy = type;
        this.trace = trace;
        this.path = path;
        this.capacity = capacity;
        this.block = block;
        this.numBlocks = (int) (capacity / block);
        loader = new AlluxioUnitLoader(path, block);

        switch (policy) {
            case "lru":
                cache = new LRUCache<UnitIndex, Unit>(numBlocks, loader);
                break;
            case "arc":
                cache = new ARCCache<UnitIndex, Unit>(numBlocks, loader);
                break;
            case "eva":
                cache = new EVACache<>(numBlocks, loader, 128);
            default:
        }
    }

    public void repeat(int cnt) {
        Metric metric = new Metric();
        System.out.println("================================");
        System.out.printf("policy=%s, trace=%s, path=%s, capacity=%d, block=%d, numBlocks=%d\n",
                policy, trace, path, capacity, block, numBlocks);
        LinkedList<TraceEntry> entries = Utils.loadTrace(trace);
        System.out.printf("trace size=%d\n", entries.size());
        for (int i = 0; i < cnt; i++) {
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
                    cache.get(unit);
                }

            }
            long duration = (System.currentTimeMillis() - startTick);
            System.out.printf("repeat:%d, bytesHitRatio:%f(%d/%d), duration: %d ms\n",
                    i, metric.bytesHitRatio(), metric.bytesHit, metric.bytesRead, duration);
            System.out.printf("hitRatio:%f(%d/%d), unique units=%d\n",
                    (double) metric.hit / (double) metric.request, metric.hit, metric.request,
                    unitSets.size());
        }
        System.out.println("================================");
    }

    public static void main(String[] args) {
        // <path> <trace> <cache_capacity_in_bytes> <unit_size_in_bytes> <repeat>
        System.out.println(Arrays.toString(args));
        if (args.length != 6) {
            System.out.println("Usage: <policy> <path> <trace> <cache_size_in_bytes> <cache_unit_size_in_bytes> <repeat>");
            System.exit(1);
        }
        String policy = args[0];
        String path = args[1];
        String trace = args[2];
        int capacity = (int) (Double.parseDouble(args[3].replace("g", "")) * 1024 * 1024 * 1024);
        int unit = Integer.parseInt(args[4]);
        int repeat = Integer.parseInt(args[5]);
        System.out.printf("capacity=%d bytes, unit=%d bytes, repeat=%d\n", capacity, unit, repeat);
        int numUnit = capacity / unit;
        System.out.printf("NumUnit=%d\n", numUnit);

        CacheBenchmark benchmark = new CacheBenchmark(policy, trace, path, capacity, unit);
        benchmark.repeat(2);
    }

}
