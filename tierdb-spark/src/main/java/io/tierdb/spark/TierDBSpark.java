package io.tierdb.spark;

import io.tierdb.connector.seam.SeamClient;
import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.spark.seam.SeamDeleter;
import io.tierdb.spark.seam.SeamRead;
import io.tierdb.spark.seam.SeamWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class TierDBSpark {

    private TierDBSpark() {}

    public static SeamRead read(SparkSession spark, SeamOptions options) {
        return new SeamRead(spark, options, SeamClient.capture(options, true));
    }

    public static void write(Dataset<Row> rows, SeamOptions options) {
        SeamWriter.write(rows, options);
    }

    public static void delete(Dataset<Row> keys, SeamOptions options) {
        SeamDeleter.delete(keys, options);
    }
}
