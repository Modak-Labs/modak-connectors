package io.modak.connector;

import io.modak.connector.seam.SeamOptions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public record ConnectorConfig(
        String jdbcUrl,
        String jdbcUser,
        String jdbcPassword,
        Duration pinTtl,
        Map<String, String> fileIoProperties) {

    private static final String FILEIO_PREFIX = "modak.fileio.";

    public static ConnectorConfig fromMap(Map<String, String> config) {
        String jdbcUrl = config.get("modak.jdbc-url");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("catalog property modak.jdbc-url is required");
        }
        Duration pinTtl = Duration.ofSeconds(
                Long.parseLong(config.getOrDefault("modak.pin-ttl-seconds", "900")));
        Map<String, String> fileIo = new HashMap<>();
        for (Map.Entry<String, String> e : config.entrySet()) {
            if (e.getKey().startsWith(FILEIO_PREFIX)) {
                fileIo.put(e.getKey().substring(FILEIO_PREFIX.length()), e.getValue());
            }
        }
        return new ConnectorConfig(jdbcUrl, config.get("modak.jdbc-user"),
                config.get("modak.jdbc-password"), pinTtl, Map.copyOf(fileIo));
    }

    public SeamOptions seamOptions(String table) {
        SeamOptions.Builder builder = SeamOptions.builder()
                .jdbcUrl(jdbcUrl)
                .table(table)
                .pinTtl(pinTtl);
        if (jdbcUser != null) {
            builder.jdbcProperty("user", jdbcUser);
        }
        if (jdbcPassword != null) {
            builder.jdbcProperty("password", jdbcPassword);
        }
        return builder.build();
    }

    public Properties jdbcProperties() {
        Properties props = new Properties();
        if (jdbcUser != null) {
            props.setProperty("user", jdbcUser);
        }
        if (jdbcPassword != null) {
            props.setProperty("password", jdbcPassword);
        }
        return props;
    }
}
