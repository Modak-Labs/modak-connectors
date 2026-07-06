package io.modak.spark.seam;

import io.modak.connector.seam.SeamClient;
import io.modak.connector.seam.SeamOptions;
import io.modak.connector.seam.SeamState;
import io.modak.connector.seam.TierKeySql;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public final class SeamRead implements AutoCloseable {

    private final SparkSession spark;
    private final SeamOptions options;
    private final SeamState state;
    private Dataset<Row> dataframe;
    private boolean closed;

    public SeamRead(SparkSession spark, SeamOptions options, SeamState state) {
        this.spark = spark;
        this.options = options;
        this.state = state;
    }

    public Dataset<Row> dataframe() {
        if (dataframe == null) {
            dataframe = build();
        }
        return dataframe;
    }

    @Override
    public void close() {
        if (!closed && state.pinId() != null) {
            SeamClient.release(options, state.pinId());
        }
        closed = true;
    }

    private Dataset<Row> build() {
        if (state.heapIsComplete() && state.hybridSeam() == null) {
            return jdbc("(SELECT * FROM " + options.qualifiedName() + ") modak_hot");
        }

        Dataset<Row> hot = jdbc("(SELECT * FROM " + options.qualifiedName()
                + " WHERE " + state.tierKeyCol() + " >= "
                + TierKeySql.literal(state.tierKeyType(), state.readSeam()) + ") modak_hot");
        if (state.snapshotId() == null) {
            return hot;
        }

        Dataset<Row> cold = coldBranch();
        if (state.hybridSeam() != null) {
            cold = cold.filter(cold.col(state.tierKeyCol())
                    .lt(TierKeyColumns.boundary(state.tierKeyType(), state.readSeam())));
        }

        Dataset<Row> delta = jdbc("(SELECT pk AS __modak_pk, op AS __modak_op,"
                + " payload::text AS __modak_payload"
                + " FROM modak.delta WHERE table_id = " + state.tableId() + ") modak_delta");

        Column coldPk = PkColumns.expression(state.primaryKeyCols(), cold);
        Dataset<Row> survivors = cold.join(delta,
                coldPk.equalTo(delta.col("__modak_pk")), "left_anti");
        Dataset<Row> upserts = delta
                .filter(delta.col("__modak_op").equalTo(0))
                .select(functions.from_json(delta.col("__modak_payload"), cold.schema())
                        .as("__modak_row"))
                .select("__modak_row.*");

        return hot.unionByName(survivors.unionByName(upserts));
    }

    private Dataset<Row> coldBranch() {
        if (!"iceberg".equals(state.lakeFormat())) {
            throw new UnsupportedOperationException("modak-spark cannot read lake format '"
                    + state.lakeFormat() + "' yet (supported: iceberg)");
        }
        String ref = options.lakeTable() != null ? options.lakeTable() : state.lakeTableRef();
        DataFrameReader reader = spark.read()
                .format("iceberg")
                .option("snapshot-id", state.snapshotId());
        return isPath(ref) ? reader.load(ref) : reader.table(ref);
    }

    private static boolean isPath(String ref) {
        return ref.contains("://") || ref.startsWith("/");
    }

    private Dataset<Row> jdbc(String table) {
        java.util.Properties props = (java.util.Properties) options.jdbcProperties().clone();
        props.putIfAbsent("driver", "org.postgresql.Driver");
        return spark.read().jdbc(options.jdbcUrl(), table, props);
    }
}
