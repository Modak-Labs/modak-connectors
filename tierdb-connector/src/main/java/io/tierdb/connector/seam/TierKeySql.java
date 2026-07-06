package io.tierdb.connector.seam;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * Renders canonical i64 tier keys back to native SQL, the connector-side
 * mirror of the worker's TierKeyType codec.
 */
public final class TierKeySql {

    private static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT);

    private TierKeySql() {}

    public static String literal(String tierKeyType, long canonical) {
        return switch (normalize(tierKeyType)) {
            case "bigint" -> Long.toString(canonical);
            case "timestamptz" -> "TIMESTAMPTZ '" + TS.format(timestamp(canonical)) + "+00'";
            case "timestamp" -> "TIMESTAMP '" + TS.format(timestamp(canonical)) + "'";
            case "date" -> "DATE '" + LocalDate.ofEpochDay(canonical) + "'";
            default -> throw new IllegalArgumentException(
                    "unsupported tier key type: " + tierKeyType);
        };
    }

    public static String canonicalExpr(String tierKeyType, String expr) {
        return switch (normalize(tierKeyType)) {
            case "bigint" -> "(" + expr + ")::bigint";
            case "timestamptz", "timestamp" ->
                    "(extract(epoch from " + expr + ") * 1000000)::bigint";
            case "date" -> "(" + expr + " - DATE '1970-01-01')::bigint";
            default -> throw new IllegalArgumentException(
                    "unsupported tier key type: " + tierKeyType);
        };
    }

    public static OffsetDateTime timestamp(long canonicalMicros) {
        return OffsetDateTime.ofInstant(
                Instant.EPOCH.plus(canonicalMicros, ChronoUnit.MICROS), ZoneOffset.UTC);
    }

    private static String normalize(String type) {
        return type == null ? "bigint" : type.trim().toLowerCase(Locale.ROOT);
    }
}
