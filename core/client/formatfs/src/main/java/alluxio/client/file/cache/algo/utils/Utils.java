package alluxio.client.file.cache.algo.utils;

import alluxio.client.file.cache.algo.benchmark.TraceEntry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;

public class Utils {

    public static LinkedList<TraceEntry> loadTrace(String path) {
        LinkedList<TraceEntry> entries = new LinkedList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                String[] tokens = line.split(",");
                if (tokens[3].equals("Read")) {
                    long offset = Long.parseLong(tokens[4]);
                    int size = Integer.parseInt(tokens[5]);
                    entries.add(new TraceEntry(offset, size));
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            System.out.println("Failed to read trace " + path);
            e.printStackTrace();
            System.exit(1);
        }
        return  entries;
    }
}
