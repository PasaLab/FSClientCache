package alluxio.client.file.cache.algo.cache.eva;

import alluxio.client.file.cache.algo.cache.AbstractCache;
import alluxio.client.file.cache.algo.cache.Loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class EVACache<K, V> extends AbstractCache<K, V> {
    public static final int NONRESUED = 0;
    public static final int RESUED = 1;

    public int maxAge;
    public int ageScaling = 1;
    public double ewmaDecay = 0.8; // Exponentially Weighted Moving Average
    ArrayList<BiasClass> classes;
    HashMap<K, V> items;
    HashMap<K, Integer> classIds;
    HashMap<K, Integer> timestamp;
    int now;
    int nextUpdate;

    boolean verbose;

    public EVACache(int capacity, Loader<K, V> loader, int maxAge) {
        super(capacity, loader);
        items = new HashMap<>();
        this.maxAge = maxAge;
        now = 0;
        nextUpdate = maxAge;
        classes = new ArrayList<>();
        classes.add(new BiasClass(maxAge)); // NON-REUSED
        classes.add(new BiasClass(maxAge)); // REUSED
        classIds = new HashMap<>();
        timestamp = new HashMap<>();
        verbose = false;
    }

    public EVACache(int capacity, Loader<K, V> loader, int maxAge, double ewmaDecay, boolean verbose) {
        this(capacity, loader, maxAge);
        this.ewmaDecay = ewmaDecay;
        this.verbose = verbose;
    }

    public void update(K k) {
        boolean present = isPresent(k);
        if (present) {
            BiasClass clazz = getBiasClass(k);
            int a = age(k);
            clazz.hits[a] += 1;
        }
        // update age
        now++;
        timestamp.put(k, now);
        nextUpdate--;
        if (nextUpdate == 0) {
            reconfigure();
            reset();
            nextUpdate += maxAge;
        }
        if (present) {
            classIds.put(k, RESUED);
        } else {
            classIds.put(k, NONRESUED);
        }
    }

    public void replaced(K k) {
        boolean present = isPresent(k);
        if (present) {
            BiasClass clazz = getBiasClass(k);
            int a = age(k);
            clazz.evictions[a] += 1;
        }
        timestamp.put(k, 0);
        classIds.put(k, NONRESUED);
    }

    /**
     * Evict one
     * @return
     */
    public void evict() {
        K bestCand = null;
        double bestRank = Double.MAX_VALUE;
        for (K k : items.keySet()) {
            int a = age(k);
            double rank = getBiasClass(k).ranks[a];
            if (rank < bestRank) {
                bestRank = rank;
                bestCand = k;
            }
        }
        if (bestCand == null) {
            System.out.println("Evict: candidate should not be null");
            System.exit(1);
        }
        if (verbose) {
            System.out.printf("now=%d, size/capacity=%d/%d\n", now, size(), capacity);
            System.out.printf("evict: bestCand=%s, bestRank=%f\n", bestCand, bestRank);
        }
        replaced(bestCand);
        remove(bestCand);
    }

    public void remove(K k) {
        items.remove(k);
        timestamp.remove(k);
        classIds.remove(k);
    }

    private BiasClass getBiasClass(K k) {
        Integer type = classIds.get(k);
        if (type == null) {
            System.out.println("classIds should not be null");
            System.exit(1);
        }
        return classes.get(type);
    }

    private boolean isPresent(K k) {
        Integer a = timestamp.get(k);
        return a != null && a != 0 ;
    }

    private int age(K k) {
        return (now - timestamp.get(k)) % maxAge;
    }

    private void reconfigure() {
        // 1. compute hit rates
        int ewmaHits = 0;
        int ewmaEvections = 0;
        for (BiasClass clazz : classes) {
            clazz.update();
            ewmaHits += clazz.totalEwmaHits();
            ewmaEvections += clazz.totalEwmaEvictions();
        }
        double lineGain = 1. * ewmaHits / (ewmaHits + ewmaEvections) / capacity;

        if (verbose) {
            System.out.printf("now=%d, lineGain=%f\n", now, lineGain);
        }

        for (BiasClass clazz : classes) {
            clazz.reconfigure(lineGain);
            clazz.ranks[maxAge -1] = -Double.MAX_VALUE;
        }

        int nonReusedHits = classes.get(NONRESUED).totalEwmaHits();
        int nonReusedEvictions = classes.get(NONRESUED).totalEwmaEvictions();
        double nonReusedMissRate = 1. * nonReusedEvictions / (nonReusedHits + nonReusedEvictions);

        int reusedHits = classes.get(RESUED).totalEwmaHits();
        int reusedEvictions = classes.get(RESUED).totalEwmaEvictions();
        double reusedMissRate = 1. * reusedEvictions / (reusedHits + reusedEvictions);

        int totalHits = reusedHits + nonReusedHits;
        int totalEvictions = reusedEvictions + nonReusedEvictions;
        double averageMissRate = 1. * totalEvictions / (totalHits + totalEvictions);

        double reusedLifetimeBias = classes.get(RESUED).ranks[0];

        if(verbose) {
            System.out.printf("now=%d, nonReusedHits=%d, nonReusedEvictions=%d; reusedHits=%d, reusedEvictions=%d;\n",
                    now, nonReusedHits, nonReusedEvictions, reusedHits, reusedEvictions);
            System.out.printf("now=%d, averageMissRate=%f, reusedMissRate=%f, reusedLifetimeBias=%f\n",
                    now, averageMissRate, reusedMissRate, reusedLifetimeBias);
        }

        for (BiasClass cl : classes) {
            for (int a = maxAge - 1; a > 0; a--) {
                double gain = (averageMissRate - (1 - cl.hitProbability[a])) / reusedMissRate * reusedLifetimeBias;
                if (!Double.isNaN(gain)) {
                    cl.ranks[a] += gain;
                } else{
                    if (verbose) {
                        System.out.printf("now=%d, a=%d, gain=%f\n", now, a, gain);
                    }
                }
            }
        }

        if (verbose) {
            System.out.printf("now=%d, after reconfigure, ranks=:\n", now);
            System.out.println(Arrays.toString(classes.get(NONRESUED).ranks));
        }

    }

    private void reset() {
        for (BiasClass cl : classes) {
            cl.reset();
        }
    }

    @Override
    public boolean isCached(K key) {
        return items.containsKey(key);
    }

    @Override
    public int size() {
        return items.size();
    }

    @Override
    public V get(K key) {
        if (isCached(key)) {
            // on object hit
            update(key);
            return items.get(key);
        } else {
            // on object miss
            V val = loader.load(key);
            while (items.size() >= capacity) {
                // evict one object because cache is full
                evict();
                if (verbose) {
                    System.out.printf("now=%d, replace with %s\n", now, key);
                }
            }
            items.put(key, val);
            timestamp.put(key, 0);
            classIds.put(key, NONRESUED);
            return val;
        }
    }

    class BiasClass {
        int maxAge;
        public double[] ranks;
        public int[] hits;
        public int[] evictions;
        public int[] ewmaHits;
        public int[] ewmaEvictions;
        public double[] hitProbability;

        public BiasClass(int maxAge) {
            this.maxAge = maxAge;
            ranks = new double[maxAge +1];
            hits = new int[maxAge + 1];
            evictions = new int[maxAge + 1];
            ewmaHits = new int[maxAge +1];
            ewmaEvictions = new int[maxAge +1];
            hitProbability = new double[maxAge +1];
            for (int i=0; i < maxAge; i++) {
                ewmaHits[i] = 0;
                ewmaEvictions[i] = 0;
                hits[i] = 0;
                evictions[i] = 0;
            }
        }

        public void reconfigure(double lineGain) {
            double[] events = new double[maxAge];
            double[] totalEventsAbove = new double[maxAge +1];
            totalEventsAbove[maxAge] = 0;
            for (int a = maxAge -1; a > 0; a--) {
                events[a] = ewmaHits[a] + ewmaEvictions[a];
                totalEventsAbove[a] = totalEventsAbove[a+1] + events[a];
            }
            int a = maxAge -1;
            hitProbability[a] = (totalEventsAbove[a] > 1e-2) ? 0.5*ewmaHits[a]/totalEventsAbove[a] : 0.0;
            double expectedLifetime = 1.0; // no age scaling
            double expectedLifetimeUnconditioned = totalEventsAbove[a];
            int totalHitsAbove = ewmaHits[a];
            double opportunityCost = 0.0;
            if (!Double.isNaN(lineGain)) {
                opportunityCost = lineGain * expectedLifetime;
                ranks[a] = hitProbability[a] - opportunityCost;
            }

            for (a= maxAge-2; a > 0; a--) {
                if (totalEventsAbove[a] > 1e-2) {
                    hitProbability[a] = (0.5 * ewmaHits[a] + totalHitsAbove) / (0.5 * events[a] + totalEventsAbove[a+1]);
                    expectedLifetime = ((1./6) * 1.0 * events[a] + expectedLifetimeUnconditioned) / (0.5 * events[a] + totalEventsAbove[a+1]);
                } else {
                    hitProbability[a] = 0.0;
                    expectedLifetime = 0.0;
                }
                totalHitsAbove += ewmaHits[a];
                expectedLifetimeUnconditioned += ageScaling * totalEventsAbove[a];
                if (!Double.isNaN(lineGain)) {
                    opportunityCost = lineGain * expectedLifetime;
                } else {
                    opportunityCost = 0.0;
                }
                ranks[a] = hitProbability[a] - opportunityCost;
            }
        }

        public void update() {
            for (int a = 0; a < maxAge; a++) {
                ewmaHits[a] *= ewmaDecay;
                ewmaHits[a] += hits[a];

                ewmaEvictions[a] *= ewmaDecay;
                ewmaEvictions[a] += evictions[a];
            }
        }

        public void reset() {
            for (int i=0; i < maxAge; i++) {
                hits[i] = 0;
                evictions[i] = 0;
            }
        }

        public int totalEwmaHits() {
            int hits = 0;
            for (int i=0; i < maxAge; i++) {
                hits += ewmaHits[i];
            }
            return hits;
        }

        public int totalEwmaEvictions() {
            int evictions = 0;
            for (int i=0; i < maxAge; i++) {
                evictions += ewmaEvictions[i];
            }
            return evictions;
        }
    }
}
