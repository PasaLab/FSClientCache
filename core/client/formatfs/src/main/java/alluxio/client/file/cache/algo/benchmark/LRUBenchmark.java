package alluxio.client.file.cache.algo.benchmark;

import alluxio.client.file.cache.algo.cache.Metric;
import alluxio.client.file.cache.algo.cache.lru.LRUCache;
import alluxio.client.file.cache.algo.utils.Utils;

import java.util.Arrays;
import java.util.LinkedList;

public class LRUBenchmark extends LRUCache<TraceEntry, Content> {
    Metric metric;

    public LRUBenchmark(int capacity, AlluxioLoader loader) {
        super(capacity, loader);
        metric = new Metric();
    }

    @Override
    public double getHitRatio() {
        return (double) metric.bytesHit / (double) metric.bytesRead;
    }

    @Override
    public void updateMetric(TraceEntry key, Content value, boolean hit) {
        if (hit) {
            metric.bytesHit += key.size;
        }
        metric.bytesRead += key.size;
    }

    public void resetMetric() {
        metric.reset();
    }

    public Metric getMetric() {
        return metric;
    }

    public static void main(String[] args) {
// <path> <trace> <cache_capacity_in_bytes> <unit_size_in_bytes> <repeat>
        System.out.println(Arrays.toString(args));
        if (args.length != 5) {
            System.out.println("Usage: <path> <trace> <cache_size_in_bytes> <cache_unit_size_in_bytes> <repeat>");
            System.exit(1);
        }
        String path = args[0];
        String trace = args[1];
        int capacity = (int) (Double.parseDouble(args[2].replace("g", "")) * 1024 * 1024 * 1024);
        int unit = Integer.parseInt(args[3]);
        int repeat = Integer.parseInt(args[4]);
        System.out.printf("capacity=%d bytes, unit=%d bytes, repeat=%d\n", capacity, unit, repeat);
        int numUnit = capacity/unit;
        System.out.printf("NumUnit=%d\n", numUnit);

        AlluxioLoader loader = new AlluxioLoader(path, unit);
        LRUBenchmark cache = new LRUBenchmark(capacity/unit, loader);

        LinkedList<TraceEntry> entries = Utils.loadTrace(trace);
        System.out.printf("trace: lines=%d\n", entries.size());

        for (int i=0; i < repeat; i++) {
            cache.resetMetric();
            long startTick= System.currentTimeMillis();
            for (TraceEntry entry : entries) {
                cache.get(entry);
            }
            long duration = (System.currentTimeMillis() - startTick);
            Metric metric = cache.getMetric();
            System.out.printf("repeat=%d, Hit Ratio: (%d/%d)=%f, duration %d ms\n",
                    i, metric.bytesHit, metric.bytesRead, cache.getHitRatio(), duration);
        }
    }
}
