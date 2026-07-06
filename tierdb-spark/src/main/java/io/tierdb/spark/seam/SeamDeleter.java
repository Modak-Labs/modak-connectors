package io.tierdb.spark.seam;

import io.tierdb.connector.seam.SeamClient;
import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.connector.seam.SeamState;
import io.tierdb.load.DeltaLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public final class SeamDeleter {

    private SeamDeleter() {}

    public static void delete(Dataset<Row> keys, SeamOptions options) {
        SeamState state = SeamClient.capture(options, false);
        TierRouter.Routed routed = TierRouter.route(keys, options, state, "delete on", "targets");

        List<String> pkCols = state.primaryKeyCols();
        Dataset<Row> hot = routed.hot().select(pkCols.stream()
                .map(c -> keys.col(c).cast("string"))
                .toArray(Column[]::new));
        hot.foreachPartition(new HotDelete(options.jdbcUrl(), options.jdbcProperties(),
                options.schemaName(), options.tableName(), pkCols));

        Dataset<Row> cold = routed.cold();
        Column[] pkFields = pkCols.stream().map(cold::col).toArray(Column[]::new);
        Dataset<Row> encoded = cold.select(
                PkColumns.expression(pkCols, cold).as("pk"),
                TierKeyColumns.canonical(cold.col(state.tierKeyCol()),
                        state.tierKeyType()).as("tier_key"),
                functions.to_json(functions.struct(pkFields)).as("payload"));
        encoded.foreachPartition(new DeltaTombstone(
                options.jdbcUrl(), options.jdbcProperties(), state.tableId()));
    }

    private static final class HotDelete implements ForeachPartitionFunction<Row> {

        private static final int BATCH = 500;

        private final String url;
        private final Properties properties;
        private final String sql;
        private final int pkCount;

        HotDelete(String url, Properties properties, String schema, String table,
                List<String> pkCols) {
            this.url = url;
            this.properties = properties;
            this.pkCount = pkCols.size();
            StringBuilder where = new StringBuilder();
            for (int i = 0; i < pkCols.size(); i++) {
                if (i > 0) {
                    where.append(" AND ");
                }
                where.append(ident(pkCols.get(i))).append("::text = ?");
            }
            this.sql = "DELETE FROM " + ident(schema) + "." + ident(table)
                    + " WHERE " + where;
        }

        @Override
        public void call(Iterator<Row> rows) throws Exception {
            if (!rows.hasNext()) {
                return;
            }
            try (Connection c = DriverManager.getConnection(url, properties)) {
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    int pending = 0;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        for (int i = 0; i < pkCount; i++) {
                            ps.setString(i + 1, row.getString(i));
                        }
                        ps.addBatch();
                        if (++pending == BATCH) {
                            ps.executeBatch();
                            pending = 0;
                        }
                    }
                    if (pending > 0) {
                        ps.executeBatch();
                    }
                }
                c.commit();
            }
        }

        private static String ident(String name) {
            return "\"" + name.replace("\"", "\"\"") + "\"";
        }
    }

    private static final class DeltaTombstone implements ForeachPartitionFunction<Row> {

        private final String url;
        private final Properties properties;
        private final long tableId;

        DeltaTombstone(String url, Properties properties, long tableId) {
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
                        return DeltaLoader.Entry.tombstone(
                                row.getString(0), row.getLong(1), row.getString(2));
                    }
                });
                c.commit();
            }
        }
    }
}
