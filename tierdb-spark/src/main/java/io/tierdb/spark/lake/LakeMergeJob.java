package io.tierdb.spark.lake;

import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.connector.seam.SeamState;
import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeMerge;
import io.tierdb.lake.access.MergeFileWriter;
import io.tierdb.lake.access.MergeWriterFactory;
import io.tierdb.lake.commit.LakeCommitLock;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * The direct-mode cold leg for Spark: partitions write merge files through the
 * format's serializable writer factory, then the driver commits every result
 * as one atomic snapshot under the shared per-table advisory lock (the commit
 * cannot rebase, so it must be serialized against the pg extension, the DuckDB
 * connector, and the worker).
 */
public final class LakeMergeJob {

    private LakeMergeJob() {}

    public static void upserts(Dataset<Row> rows, SeamState state, SeamOptions options) {
        run(rows, state, options, false);
    }

    /** Keys need the PK columns and the tier key column. */
    public static void deletes(Dataset<Row> keys, SeamState state, SeamOptions options) {
        run(keys, state, options, true);
    }

    private static void run(Dataset<Row> rows, SeamState state, SeamOptions options,
            boolean delete) {
        LakeAccess access = state.lake().openAccess(Map.of());
        execute(access.merge(state.lake().tableRef(), List.of(rows.columns()),
                        state.table().primaryKeyCols()),
                rows, state, options, delete);
    }

    private static <F> void execute(LakeMerge<F> merge, Dataset<Row> rows, SeamState state,
            SeamOptions options, boolean delete) {
        MergeWriterFactory<F> factory = merge.writerFactory();
        List<F> results = rows.javaRDD()
                .mapPartitions(it -> writePartition(factory, it, delete))
                .collect();
        if (results.isEmpty()) {
            return;
        }
        boolean committed = false;
        try {
            try (LakeCommitLock lock = LakeCommitLock.acquire(
                    DriverManager.getConnection(options.jdbcUrl(), options.jdbcProperties()),
                    state.table().tableId())) {
                merge.commit(results);
                committed = true;
            }
        } catch (Exception e) {
            throw new IllegalStateException("lake merge failed for "
                    + options.qualifiedName(), e);
        } finally {
            if (!committed) {
                abortQuietly(merge, results);
            }
        }
    }

    private static <F> Iterator<F> writePartition(MergeWriterFactory<F> factory,
            Iterator<Row> rows, boolean delete) throws Exception {
        if (!rows.hasNext()) {
            return Collections.emptyIterator();
        }
        MergeFileWriter<F> writer = factory.newWriter();
        try {
            while (rows.hasNext()) {
                Object[] row = neutral(rows.next());
                if (delete) {
                    writer.delete(row);
                } else {
                    writer.upsert(row);
                }
            }
            return List.of(writer.complete()).iterator();
        } catch (Exception e) {
            writer.close();
            throw e;
        }
    }

    private static <F> void abortQuietly(LakeMerge<F> merge, List<F> results) {
        try {
            merge.abort(results);
        } catch (Exception ignored) {
        }
    }

    private static Object[] neutral(Row row) {
        Object[] out = new Object[row.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = neutral(row.get(i));
        }
        return out;
    }

    private static Object neutral(Object value) {
        if (value instanceof Timestamp ts) {
            return ts.toInstant().atOffset(ZoneOffset.UTC);
        }
        if (value instanceof java.time.Instant instant) {
            return instant.atOffset(ZoneOffset.UTC);
        }
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate();
        }
        return value;
    }
}
