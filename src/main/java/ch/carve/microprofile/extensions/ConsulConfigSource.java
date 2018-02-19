package ch.carve.microprofile.extensions;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.eclipse.microprofile.config.spi.ConfigSource;

import com.ecwid.consul.v1.ConsulClient;

public class ConsulConfigSource implements ConfigSource {

    private ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();
    private ConsulClient client = new ConsulClient(getEnvOrSystemProperty("consul.host", "localhost"));

    @Override
    public Map<String, String> getProperties() {
        return cache;
    }

    @Override
    public String getValue(String propertyName) {
        return cache.computeIfAbsent(propertyName, k -> client.getKVValue(k).getValue().getDecodedValue());
    }

    @Override
    public String getName() {
        return "ConsulConfigSource";
    }

    @Override
    public int getOrdinal() {
        return 120;
    }

    private static String getEnvOrSystemProperty(String key, String defaultValue) {
        return Optional.of(System.getenv(key)).orElse(System.getProperty(key, defaultValue));
    }
}
