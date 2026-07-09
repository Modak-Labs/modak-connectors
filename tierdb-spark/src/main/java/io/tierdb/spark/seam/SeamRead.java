package io.tierdb.spark.seam;

import io.tierdb.connector.read.Cold;
import io.tierdb.connector.read.Read;
import io.tierdb.connector.seam.SeamClient;
import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.connector.seam.SeamState;
import io.tierdb.connector.seam.TierKeySql;
import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeScan;
import io.tierdb.spark.lake.SparkLakeReaders;
import java.util.Map;
import org.apache.spark.sql.Column;
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
        Read read = resolve();
        if (read instanceof Read.Heap) {
            return jdbc("(SELECT * FROM " + options.qualifiedName() + ") tierdb_hot");
        }
        Read.Seam seam = (Read.Seam) read;

        Dataset<Row> hot = jdbc("(SELECT * FROM " + options.qualifiedName()
                + " WHERE " + state.table().tierKeyCol() + " >= "
                + TierKeySql.literal(state.table().tierKeyType(), seam.t()) + ") tierdb_hot");

        Cold cold = seam.cold();
        if (cold instanceof Cold.Delta) {
            return hot.unionByName(deltaUpserts(hot));
        }
        if (cold instanceof Cold.Live live) {
            return hot.unionByName(clip(coldBranch(live.scan()), seam.t()));
        }
        Cold.Merge merge = (Cold.Merge) cold;
        return hot.unionByName(mergeDelta(clip(coldBranch(merge.scan()), seam.t()), hot));
    }

    private Read resolve() {
        boolean hybrid = state.cutLine().hybridSeam() != null;
        // No access plugin is needed until a snapshot has been pinned.
        if (!hybrid && !state.mode().isDirect() && state.cutLine().lakeProps().isEmpty()) {
            return state.mode().heapComplete() ? new Read.Heap()
                    : new Read.Seam(state.readSeam(), new Cold.Delta());
        }
        LakeAccess access = state.lake().openAccess(Map.of());
        return hybrid ? state.scanHybrid(access) : state.scan(access);
    }

    private Dataset<Row> clip(Dataset<Row> cold, long seam) {
        if (state.cutLine().hybridSeam() == null) {
            return cold;
        }
        return cold.filter(cold.col(state.table().tierKeyCol())
                .lt(TierKeyColumns.boundary(state.table().tierKeyType(), seam)));
    }

    private Dataset<Row> mergeDelta(Dataset<Row> cold, Dataset<Row> hot) {
        Dataset<Row> delta = jdbc("(SELECT pk AS __tierdb_pk, op AS __tierdb_op,"
                + " payload::text AS __tierdb_payload"
                + " FROM tierdb.delta WHERE table_id = " + state.table().tableId() + ") tierdb_delta");
        Column coldPk = PkColumns.expression(state.table().primaryKeyCols(), cold);
        Dataset<Row> survivors = cold.join(delta,
                coldPk.equalTo(delta.col("__tierdb_pk")), "left_anti");
        Dataset<Row> upserts = delta
                .filter(delta.col("__tierdb_op").equalTo(0))
                .select(functions.from_json(delta.col("__tierdb_payload"), hot.schema())
                        .as("__tierdb_row"))
                .select("__tierdb_row.*");
        return survivors.unionByName(upserts);
    }

    private Dataset<Row> deltaUpserts(Dataset<Row> hot) {
        Dataset<Row> delta = jdbc("(SELECT payload::text AS __tierdb_payload"
                + " FROM tierdb.delta WHERE table_id = " + state.table().tableId()
                + " AND op = 0) tierdb_delta");
        return delta
                .select(functions.from_json(delta.col("__tierdb_payload"), hot.schema())
                        .as("__tierdb_row"))
                .select("__tierdb_row.*");
    }

    private Dataset<Row> coldBranch(LakeScan scan) {
        String ref = options.lakeTable() != null ? options.lakeTable() : state.lake().tableRef();
        return SparkLakeReaders.forFormat(state.lake().format()).read(spark, ref, scan);
    }

    private Dataset<Row> jdbc(String table) {
        java.util.Properties props = (java.util.Properties) options.jdbcProperties().clone();
        props.putIfAbsent("driver", "org.postgresql.Driver");
        return spark.read().jdbc(options.jdbcUrl(), table, props);
    }
}
