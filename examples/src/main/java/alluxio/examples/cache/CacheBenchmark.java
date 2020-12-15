package alluxio.examples.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.*;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.core.*;
import org.apache.commons.lang3.RandomUtils;

import java.util.*;

public class CacheBenchmark {

    static class TraceEntry {
        public long off;
        public int size;
        public TraceEntry(long off, int size) {
            this.off = off;
            this.size = size;
        }
    }

    //  0               1               2               3           4       5
    // <alluxioURI>  <cachePolicy> <cacheCapacity> <blockSize>    <mode>   <giga>
    public static void main(String[] args) {
        if (args.length != 6) {
            System.out.println("Usage: <alluxioURI> <cachePolicy> <cacheCapacity> <blockSize> <mode> <giga>");
            System.exit(1);
        }
        AlluxioURI alluxioURI = new AlluxioURI(args[0]);
        String policy = args[1];
        String cachePacity = args[2];
        int blockSize = Integer.parseInt(args[3]);
        String mode = args[4];
        Double cacheInGiga = Double.parseDouble(args[5]);

        initSetting(policy, cachePacity, blockSize, mode);

        List<TraceEntry> traces = genTrance(cacheInGiga);

	// warm
        randomTest(alluxioURI, traces);

        ClientCacheStatistics.INSTANCE.clear();
        long startTick = System.currentTimeMillis();
        randomTest(alluxioURI, traces);
        long duration = (System.currentTimeMillis() - startTick);
        System.out.println("Statistics:");
        System.out.println("usedTime: " + duration);
        collectStats();

        System.gc();
        System.exit(0);
    }

    static void initSetting(String policy, String cacheCapacity, int blockSize, String mode) {
        if (mode.equals("promote")) {
            CacheParamSetter.mode = ClientCacheContext.MODE.PROMOTE;
        } else {
            CacheParamSetter.mode = ClientCacheContext.MODE.EVICT;
        }
        CacheParamSetter.CACHE_SIZE = blockSize;
        switch (policy) {
            case "isk":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.ISK;
                break;
            case "gr":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.GR;
                break;
            case "lfu":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.TRADITIONAL_LFU;
                break;
            case "lru":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.TRADITIONAL_LRU;
                break;
            case "divide":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.DIVIDE_GR;
                break;
            case "arc":
                CacheParamSetter.POLICY_NAME = CachePolicy.PolicyName.ARC;
                break;
        }
        CacheParamSetter.mCacheSpaceLimit = cacheCapacity;
    }

    static List<TraceEntry> genTrance(double gigasize) {
        List<TraceEntry> traces = new LinkedList<TraceEntry>();
        int nTraces = (int) (gigasize * 1000);
        for (int i = 0; i < nTraces; i++) {
            long offset = RandomUtils.nextLong(0, (long) (gigasize * 1024 * 1024 * 1024) - 4 * 1024 * 1024);
            int size = RandomUtils.nextInt(8 * 1024, 4 * 1024 * 1024);
            traces.add(new TraceEntry(offset, size));
        }
        return traces;
    }

    static void randomTest(AlluxioURI path, List<TraceEntry> traces) {
        long preciseTime = 0;
        FileSystem fs = CacheFileSystem.get(true);

        try {
            long readCount = 0;
            FileInStream in = fs.openFile(path);
            for (TraceEntry entry : traces) {
                readCount += 1;
                long offset = entry.off;
                int size = entry.size;
                byte[] buffer = new byte[size];
                try {
                    long a = System.currentTimeMillis();
                    int readedLen = in.positionedRead(offset, buffer, 0, size);
                    preciseTime += System.currentTimeMillis() - a;
                    if (readedLen == -1) {
                        throw new RuntimeException();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            in.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void collectStats() {
        System.out.println("cacheSpace: " + ClientCacheContext.INSTANCE.getCacheLimit());
        System.out.println("blockSize: " + ClientCacheContext.CACHE_SIZE);
        System.out.println("cachePolicy: " + CacheParamSetter.POLICY_NAME);
        System.out.println("BHR: " + ClientCacheStatistics.INSTANCE.hitRatio());
        System.out.println("bytesHit: " + ClientCacheStatistics.INSTANCE.bytesHit);
        System.out.println("bytesRead: " + ClientCacheStatistics.INSTANCE.bytesRead);
    }
}
