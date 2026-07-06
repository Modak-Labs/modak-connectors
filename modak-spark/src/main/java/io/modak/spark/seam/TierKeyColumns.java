package io.modak.spark.seam;

import io.modak.connector.seam.TierKeySql;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Locale;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

final class TierKeyColumns {

    private TierKeyColumns() {}

    static Column boundary(String tierKeyType, long canonical) {
        return switch (normalize(tierKeyType)) {
            case "bigint" -> functions.lit(canonical);
            case "timestamptz", "timestamp" -> functions.lit(
                    Timestamp.from(TierKeySql.timestamp(canonical).toInstant()));
            case "date" -> functions.lit(
                    java.sql.Date.valueOf(LocalDate.ofEpochDay(canonical)));
            default -> throw new IllegalArgumentException(
                    "unsupported tier key type: " + tierKeyType);
        };
    }

    static Column canonical(Column col, String tierKeyType) {
        return switch (normalize(tierKeyType)) {
            case "bigint" -> col.cast("long");
            case "timestamptz", "timestamp" -> functions.unix_micros(col.cast("timestamp"));
            case "date" -> functions.datediff(col, functions.lit("1970-01-01")).cast("long");
            default -> throw new IllegalArgumentException(
                    "unsupported tier key type: " + tierKeyType);
        };
    }

    private static String normalize(String type) {
        return type == null ? "bigint" : type.trim().toLowerCase(Locale.ROOT);
    }
}
