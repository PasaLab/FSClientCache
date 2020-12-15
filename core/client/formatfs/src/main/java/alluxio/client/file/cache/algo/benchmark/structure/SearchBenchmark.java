package alluxio.client.file.cache.algo.benchmark.structure;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

public class SearchBenchmark {
    public static final int capacity = 3000;
    public static final int searchTimes = 500000;

    public static void main(String[] args) {
        LinkedList<Integer> list = new LinkedList<>();
        HashMap<Integer, Integer> map = new HashMap<>();

        // init
        for (int i=0; i < capacity; i++) {
            list.add(i);
            map.put(i, i);
        }

        // HashMap
        long startTick = System.currentTimeMillis();
        Random random = new Random(1234);
        int hit = 0;
        int request = 0;
        for (int i=0; i < searchTimes; i++ ) {
            if (list.contains(i)) {
                hit ++;
            }
            request ++;
        }
        long duration1 = (System.currentTimeMillis() - startTick);

        startTick = System.currentTimeMillis();
        random = new Random(1234);
        hit = 0;
        request = 0;
        for (int i=0; i < searchTimes; i++ ) {
            if (map.containsKey(i)) {
                hit ++;
            }
            request ++;
        }
        long duration2 = (System.currentTimeMillis() - startTick);

        System.out.printf("LinkedList: %d ms; HashMap: %d ms\n", duration1, duration2);
    }
}
