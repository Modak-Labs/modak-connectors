package io.tierdb.spark.lake;

import io.tierdb.lake.access.LakeScan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Renders a resolved {@link LakeScan} as a Spark DataFrame. Building the cold
 * DataFrame is an engine-by-format concern, so this strategy lives beside the
 * engine rather than behind the shared lake port.
 */
public interface SparkLakeReader {

    Dataset<Row> read(SparkSession spark, String tableRef, LakeScan scan);
}
