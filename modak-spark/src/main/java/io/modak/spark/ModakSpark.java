package io.modak.spark;

import io.modak.connector.seam.SeamClient;
import io.modak.connector.seam.SeamOptions;
import io.modak.spark.seam.SeamDeleter;
import io.modak.spark.seam.SeamRead;
import io.modak.spark.seam.SeamWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class ModakSpark {

    private ModakSpark() {}

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
