package io.tierdb.spark.seam;

import io.tierdb.common.mode.InsertPlan;
import io.tierdb.common.mode.RouteTarget;
import io.tierdb.connector.seam.SeamClient;
import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.connector.seam.SeamState;
import io.tierdb.load.DeltaLoader;
import io.tierdb.spark.lake.LakeMergeJob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.Properties;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;

public final class SeamWriter {

    private SeamWriter() {}

    public static void write(Dataset<Row> rows, SeamOptions options) {
        SeamState state = SeamClient.capture(options, false);
        TierRouter.Routed routed = TierRouter.route(rows, options, state, "write to", "contains");
        InsertPlan coldPlan = state.mode().planInsert(RouteTarget.COLD);

        append(routed.hot(), options);
        Dataset<Row> cold = routed.cold();
        if (coldPlan.toHeap()) {
            append(cold, options);
        }
        coldPlan.cold().ifPresent(sink -> {
            switch (sink) {
                case LAKE -> LakeMergeJob.upserts(cold, state, options);
                case DELTA -> deltaUpsert(cold, state, options);
            }
        });
    }

    private static void deltaUpsert(Dataset<Row> cold, SeamState state, SeamOptions options) {
        Dataset<Row> encoded = cold.select(
                PkColumns.expression(state.table().primaryKeyCols(), cold).as("pk"),
                TierKeyColumns.canonical(cold.col(state.table().tierKeyCol()),
                        state.table().tierKeyType()).as("tier_key"),
                functions.to_json(functions.struct(functions.col("*"))).as("payload"));
        encoded.foreachPartition(new DeltaUpsert(
                options.jdbcUrl(), options.jdbcProperties(), state.table().tableId()));
    }

    private static void append(Dataset<Row> rows, SeamOptions options) {
        rows.write().mode(SaveMode.Append)
                .jdbc(options.jdbcUrl(), options.qualifiedName(), options.jdbcProperties());
    }

    private static final class DeltaUpsert implements ForeachPartitionFunction<Row> {

        private final String url;
        private final Properties properties;
        private final long tableId;

        DeltaUpsert(String url, Properties properties, long tableId) {
            this.url = url;
            this.properties = properties;
            this.tableId = tableId;
        }

        @Override
        public void call(Iterator<Row> rows) throws Exception {
            if (!rows.hasNext()) {
                return;
            }
            try (Connection c = DriverManager.getConnection(url, properties)) {
                c.setAutoCommit(false);
                DeltaLoader.upsert(c, tableId, new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return rows.hasNext();
                    }

                    @Override
                    public DeltaLoader.Entry next() {
                        Row row = rows.next();
                        return DeltaLoader.Entry.upsert(
                                row.getString(0), row.getLong(1), row.getString(2));
                    }
                });
                c.commit();
            }
        }
    }
}
