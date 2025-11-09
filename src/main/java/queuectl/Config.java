package queuectl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class Config {

    private static final String CONFIG_FILE = "config.properties";
    private static Properties props = new Properties();

    static {
        load();
    }

    private static void load() {
        try (InputStream input = new FileInputStream(CONFIG_FILE)) {
            props.load(input);
        } catch (IOException e) {
            props.setProperty("max_retries", "3");
            props.setProperty("backoff_base", "2");
            props.setProperty("job_timeout_seconds", "300");
            props.setProperty("lock_timeout_seconds", "60");
            save();
        }
    }

    private static void save() {
        try (OutputStream output = new FileOutputStream(CONFIG_FILE)) {
            props.store(output, null);
        } catch (IOException e) {
            System.err.println("Error saving config: " + e.getMessage());
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }

    public static int getInt(String key) {
        return getInt(key, 0);
    }

    public static int getInt(String key, int defaultValue) {
        String v = props.getProperty(key);
        if (v == null) return defaultValue;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static void set(String key, String value) {
        props.setProperty(key, value);
        save();
    }
}