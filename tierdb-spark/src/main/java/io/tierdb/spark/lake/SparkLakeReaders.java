package io.tierdb.spark.lake;

/** Selects the reader strategy for a lake format. */
public final class SparkLakeReaders {

    private SparkLakeReaders() {}

    public static SparkLakeReader forFormat(String format) {
        if ("iceberg".equals(format)) {
            return new IcebergSparkReader();
        }
        throw new UnsupportedOperationException("tierdb-spark cannot read lake format '"
                + format + "' yet (supported: iceberg)");
    }
}
