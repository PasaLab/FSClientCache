package alluxio.client.file.cache.algo.dataset;

import org.apache.commons.lang3.RandomUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class GenDataset {
    public static void main(String[] args) {
        // Usage: <Target> <GB>
        if (args.length < 2) {
            System.out.println("Usage: <Target> <GB>");
            System.exit(1);
        }
        String file = args[0];
        int gigasize = Integer.parseInt(args[1]);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            for (int i = 0; i < gigasize * 1000; i++) {
                long offset = RandomUtils.nextLong(0, 20L * 1024 * 1024 * 1024 - 4 * 1024 * 1024);
                int size = RandomUtils.nextInt(8 * 1024, 4 * 1024 * 1024);
                writer.write(String.format("1111111,proj,0,Read,%d,%d,222222\n", offset, size));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
