package alluxio.client.file.cache.algo.utils;

import java.util.HashMap;

public class Configuration {

    public HashMap<String, String> configs;

    public Configuration() {
        configs = new HashMap<>();
    }

    public Configuration(String[] args) {
        this();
        fromStrings(args);
    }

    public int getInt(String key) {
        return Integer.parseInt(configs.get(key));
    }

    public long getLong(String key) {
        return Long.parseLong(configs.get(key));
    }

    public double getDouble(String key) {
        return Double.parseDouble(configs.get(key));
    }

    public boolean getBoolean(String key) {
        return configs.get(key).equals("true") ;
    }

    public String getString(String key) {
        return configs.get(key);
    }

    public void putString(String key, String value) {
        configs.put(key, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String key : configs.keySet()) {
            sb.append(String.format("%s=%s\n", key, configs.get(key)));
        }
        return sb.toString();
    }

    public void fromStrings(String[] args) {
        for (int i=0; i < args.length; i++) {
            Pair<String, String> pair = parseParam(args[i]);
            configs.put(pair.getFist(), pair.getSecond());
        }
    }

    /**
     * Parse arguments which is in format "a=c"
     *
     * @param str   e.g. "a=c"
     * @return      e.g. Pair(a,c)
     */
    public static Pair<String, String> parseParam(String str) {
        String[] tokens = str.split("=");
        if (tokens.length != 2) {
            System.out.println("Invalid parameter format: " + str);
            System.exit(1);
        }
        return new Pair<>(tokens[0], tokens[1]);
    }
}
