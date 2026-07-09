package io.tierdb.spark.lake;

import io.tierdb.lake.access.LakeScan;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

final class IcebergSparkReader implements SparkLakeReader {

    @Override
    public Dataset<Row> read(SparkSession spark, String tableRef, LakeScan scan) {
        DataFrameReader reader = spark.read()
                .format("iceberg")
                .option("snapshot-id", scan.props().get("snapshot_id"));
        return isPath(tableRef) ? reader.load(tableRef) : reader.table(tableRef);
    }

    private static boolean isPath(String ref) {
        return ref.contains("://") || ref.startsWith("/");
    }
}
