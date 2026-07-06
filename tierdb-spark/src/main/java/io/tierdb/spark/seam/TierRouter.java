package io.tierdb.spark.seam;

import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.connector.seam.SeamState;
import io.tierdb.connector.seam.TierKeySql;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

final class TierRouter {

    record Routed(Dataset<Row> hot, Dataset<Row> cold) {}

    private TierRouter() {}

    static Routed route(Dataset<Row> rows, SeamOptions options, SeamState state,
            String action, String verb) {
        Column tierKey = rows.col(state.tierKeyCol());
        Column cutLine = TierKeyColumns.boundary(state.tierKeyType(), state.tierKeyHi());
        Dataset<Row> hot = rows.filter(tierKey.geq(cutLine));
        Dataset<Row> cold = rows.filter(tierKey.lt(cutLine));

        Long line = state.retentionLine();
        if (line != null && !cold.filter(tierKey.lt(
                TierKeyColumns.boundary(state.tierKeyType(), line))).isEmpty()) {
            throw new IllegalStateException(action + " " + options.qualifiedName() + " " + verb
                    + " rows below the retention line "
                    + TierKeySql.literal(state.tierKeyType(), line)
                    + ", rows this old have been expired from the lake");
        }
        return new Routed(hot, cold);
    }
}
