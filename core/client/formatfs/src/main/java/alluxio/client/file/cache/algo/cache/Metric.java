package alluxio.client.file.cache.algo.cache;

public class Metric {
    public long bytesRead;
    public long bytesHit;
    public long hit;
    public long request;

    public Metric() {
        reset();
    }

    public void reset() {
        bytesHit = 0;
        bytesRead = 0;
        hit = 0;
        request = 0;
    }

    public double bytesHitRatio() {
        return (double) bytesHit / (double) bytesRead;
    }

    public double hitRatio() {
        return (double) hit / (double) request;
    }
}
