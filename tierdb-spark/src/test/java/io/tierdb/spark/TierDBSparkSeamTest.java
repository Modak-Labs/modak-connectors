package io.tierdb.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.TableMode;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.lake.iceberg.IcebergLakeStoragePlugin;
import io.tierdb.spark.seam.SeamRead;
import io.tierdb.tiering.JdbcHotSource;
import io.tierdb.tiering.policy.SealGatedEvictionPolicy;
import io.tierdb.tiering.TieringWorker;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * The worker tiers a real partitioned table into Iceberg, delta rows correct
 * cold data, and Spark reads one consistent view through {@link TierDBSpark},
 * then writes back through the routed insert path.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TierDBSparkSeamTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static TableId table;
    private static String location;
    private static SparkSession spark;
    private static SeamOptions options;

    @BeforeAll
    static void setUpWorld() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);

        exec("""
                CREATE TABLE public.events (
                    id bigint NOT NULL, event_time bigint NOT NULL, val text
                ) PARTITION BY RANGE (event_time)
                """);
        exec("CREATE TABLE public.events_p0 PARTITION OF public.events FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.events_p1 PARTITION OF public.events FOR VALUES FROM (100) TO (200)");
        exec("CREATE TABLE public.events_p2 PARTITION OF public.events FOR VALUES FROM (200) TO (300)");
        exec("""
                INSERT INTO public.events VALUES
                  (1, 10, 'a'), (2, 20, 'b'),      -- p0 (cold-bound)
                  (3, 110, 'c'),                   -- p1 (cold-bound)
                  (4, 210, 'hot'), (5, 220, 'hot') -- p2 (stays hot)
                """);

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.required(2, "event_time",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(3, "val",
                        org.apache.iceberg.types.Types.StringType.get()));
        location = warehouse.resolve("events_cold").toString();
        new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
                .create(schema, org.apache.iceberg.PartitionSpec.unpartitioned(), location);

        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                relOid("public.events"), "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER, location));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        PartitionId p0 = registerPartition("events_p0", 0, 100);
        PartitionId p1 = registerPartition("events_p1", 100, 200);
        registerPartition("events_p2", 200, 300);

        new TieringWorker(catalog, new IcebergLakeStoragePlugin().create(Map.of()),
                new JdbcHotSource(dataSource), (t, now) -> List.of(p0, p1),
                new SealGatedEvictionPolicy()).runCycle(table, NOW);
        new TieringWorker(catalog, new IcebergLakeStoragePlugin().create(Map.of()),
                new JdbcHotSource(dataSource), (t, now) -> List.of(),
                new SealGatedEvictionPolicy()).runCycle(table, NOW);

        exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES ("
                + table.oid() + ", '2', 0, 20, nextval('tierdb.delta_version'),"
                + " '{\"id\": 2, \"event_time\": 20, \"val\": \"b2\"}')");
        exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES ("
                + table.oid() + ", '3', 1, 110, nextval('tierdb.delta_version'), NULL)");

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("tierdb-spark-seam-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.catalog.default_iceberg",
                        "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.default_iceberg.type", "hadoop")
                .config("spark.sql.catalog.default_iceberg.warehouse", warehouse.toString())
                .getOrCreate();

        options = SeamOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.events")
                .build();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (spark != null) {
            spark.stop();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    @Order(1)
    void pinnedReadMergesHotColdAndDelta() {
        try (SeamRead read = TierDBSpark.read(spark, options)) {
            assertEquals("1", queryOne("SELECT count(*)::text FROM tierdb.read_pins"));
            assertEquals(List.of("1|10|a", "2|20|b2", "4|210|hot", "5|220|hot"),
                    rows(read.dataframe()));
        }
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.read_pins"));
    }

    @Test
    @Order(2)
    void pinnedReadIgnoresSnapshotsCommittedAfterTheCapture() {
        try (SeamRead read = TierDBSpark.read(spark, options)) {
            spark.createDataFrame(
                            List.of(RowFactory.create(99L, 50L, "late")), eventsSchema())
                    .write().format("iceberg").mode("append").save(location);

            assertEquals(4, read.dataframe().count());
        }
    }

    @Test
    @Order(3)
    void writesRouteAcrossTheSeam() {
        Dataset<Row> batch = spark.createDataFrame(List.of(
                RowFactory.create(6L, 250L, "w-hot"),
                RowFactory.create(7L, 30L, "w-cold")), eventsSchema());

        TierDBSpark.write(batch, options);

        assertEquals("1", queryOne("SELECT count(*)::text FROM public.events WHERE id = 6"));
        assertEquals("0|w-cold", queryOne("SELECT op || '|' || (payload ->> 'val')"
                + " FROM tierdb.delta WHERE table_id = " + table.oid() + " AND pk = '7'"));

        try (SeamRead read = TierDBSpark.read(spark, options)) {
            assertEquals(List.of("1|10|a", "2|20|b2", "4|210|hot", "5|220|hot",
                    "6|250|w-hot", "7|30|w-cold"), rows(read.dataframe()));
        }
    }

    @Test
    @Order(4)
    void mirroredTablesReadAndWriteAsAPlainHeap() {
        exec("CREATE TABLE public.readings (id bigint NOT NULL, event_time bigint NOT NULL, val text)");
        exec("INSERT INTO public.readings VALUES (1, 5, 'm1'), (2, 1500, 'm2')");
        TableId mirrored = catalog.register(new TableRegistration(
                relOid("public.readings"), "public", "readings", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER,
                warehouse.resolve("readings_mirror").toString(),
                TableMode.MIRRORED, "pub_readings", "slot_readings",
                Optional.empty(), Optional.empty()));
        catalog.initCutline(mirrored, new TierKey(Long.MIN_VALUE), new LakeSnapshotId(0));

        SeamOptions mirroredOptions = SeamOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.readings")
                .build();

        try (SeamRead read = TierDBSpark.read(spark, mirroredOptions)) {
            assertEquals(List.of("1|5|m1", "2|1500|m2"), rows(read.dataframe()));
        }

        TierDBSpark.write(spark.createDataFrame(
                List.of(RowFactory.create(3L, 7L, "m3")), eventsSchema()), mirroredOptions);

        assertEquals("1", queryOne("SELECT count(*)::text FROM public.readings WHERE id = 3"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta WHERE table_id = "
                + mirrored.oid()));
        try (SeamRead read = TierDBSpark.read(spark, mirroredOptions)) {
            assertEquals(List.of("1|5|m1", "2|1500|m2", "3|7|m3"), rows(read.dataframe()));
        }
    }

    @Test
    @Order(5)
    void hybridReadSplitsAMirroredScanAtTheSeam() {
        long mirrored = Long.parseLong(queryOne(
                "SELECT table_id::text FROM tierdb.tables WHERE table_name = 'readings'"));
        String mirrorLocation = warehouse.resolve("readings_mirror").toString();

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.required(2, "event_time",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(3, "val",
                        org.apache.iceberg.types.Types.StringType.get()));
        new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
                .create(schema, org.apache.iceberg.PartitionSpec.unpartitioned(), mirrorLocation);
        spark.createDataFrame(List.of(
                        RowFactory.create(1L, 5L, "m1-lake"),
                        RowFactory.create(2L, 1500L, "m2-lake"),
                        RowFactory.create(3L, 7L, "m3-lake")), eventsSchema())
                .write().format("iceberg").mode("append").save(mirrorLocation);
        long snapshot = new org.apache.iceberg.hadoop.HadoopTables(
                new org.apache.hadoop.conf.Configuration())
                .load(mirrorLocation).currentSnapshot().snapshotId();

        exec("UPDATE tierdb.cutline SET lake_props = jsonb_build_object('snapshot_id', " + snapshot
                + ") WHERE table_id = " + mirrored);
        exec("UPDATE tierdb.cutline SET replicated_lsn = 9223372036854775807"
                + " WHERE table_id = " + mirrored);

        SeamOptions hybrid = SeamOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.readings")
                .hybrid(true)
                .build();

        try (SeamRead read = TierDBSpark.read(spark, hybrid)) {
            assertEquals(List.of("1|5|m1-lake", "2|1500|m2", "3|7|m3-lake"),
                    rows(read.dataframe()));
        }
    }

    @Test
    @Order(6)
    void hybridReadFallsBackToTheHeapWhenTheFrontierLags() {
        long mirrored = Long.parseLong(queryOne(
                "SELECT table_id::text FROM tierdb.tables WHERE table_name = 'readings'"));
        exec("UPDATE tierdb.cutline SET replicated_lsn = 0 WHERE table_id = " + mirrored);

        SeamOptions hybrid = SeamOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.readings")
                .hybrid(true)
                .mirrorWait(java.time.Duration.ofMillis(200))
                .build();

        try (SeamRead read = TierDBSpark.read(spark, hybrid)) {
            assertEquals(List.of("1|5|m1", "2|1500|m2", "3|7|m3"), rows(read.dataframe()));
        }
    }

    @Test
    @Order(7)
    void mirroredWritesBelowTheDropBoundaryBecomeDeltaRows() {
        long mirrored = Long.parseLong(queryOne(
                "SELECT table_id::text FROM tierdb.tables WHERE table_name = 'readings'"));
        exec("UPDATE tierdb.tables SET heap_retention_lag = 500 WHERE table_id = " + mirrored);
        exec("UPDATE tierdb.cutline SET tier_key_hi = 1000 WHERE table_id = " + mirrored);

        SeamOptions mirroredOptions = SeamOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.readings")
                .build();
        TierDBSpark.write(spark.createDataFrame(List.of(
                RowFactory.create(4L, 1500L, "m4-hot"),
                RowFactory.create(5L, 8L, "m5-cold")), eventsSchema()), mirroredOptions);

        assertEquals("1", queryOne("SELECT count(*)::text FROM public.readings WHERE id = 4"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM public.readings WHERE id = 5"));
        assertEquals("0|m5-cold", queryOne("SELECT op || '|' || (payload ->> 'val')"
                + " FROM tierdb.delta WHERE table_id = " + mirrored + " AND pk = '5'"));
    }

    @Test
    @Order(8)
    void writesBelowTheRetentionLineAreRejected() {
        exec("UPDATE tierdb.cutline SET retention_line = 25 WHERE table_id = " + table.oid());

        Dataset<Row> expired = spark.createDataFrame(
                List.of(RowFactory.create(8L, 10L, "too-old")), eventsSchema());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> TierDBSpark.write(expired, options));

        assertTrue(e.getMessage().contains("retention line"), e.getMessage());
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta WHERE table_id = "
                + table.oid() + " AND pk = '8'"));
    }

    @Test
    @Order(9)
    void deletesRouteAcrossTheSeam() {
        Dataset<Row> keys = spark.createDataFrame(List.of(
                RowFactory.create(6L, 250L, null),
                RowFactory.create(7L, 30L, null)), eventsSchema());

        TierDBSpark.delete(keys, options);

        assertEquals("0", queryOne("SELECT count(*)::text FROM public.events WHERE id = 6"));
        assertEquals("1|7", queryOne("SELECT op || '|' || (payload ->> 'id')"
                + " FROM tierdb.delta WHERE table_id = " + table.oid() + " AND pk = '7'"));

        try (SeamRead read = TierDBSpark.read(spark, options)) {
            assertEquals(List.of("1|10|a", "2|20|b2", "4|210|hot", "5|220|hot"),
                    rows(read.dataframe()), "both deletes disappear from the merged view");
        }
    }

    @Test
    @Order(10)
    void deletesBelowTheRetentionLineAreRejected() {
        Dataset<Row> expired = spark.createDataFrame(
                List.of(RowFactory.create(1L, 10L, null)), eventsSchema());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> TierDBSpark.delete(expired, options));

        assertTrue(e.getMessage().contains("retention line"), e.getMessage());
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta WHERE table_id = "
                + table.oid() + " AND pk = '1'"), "no tombstone written for the expired key");
    }

    private static StructType eventsSchema() {
        return new StructType()
                .add("id", DataTypes.LongType, false)
                .add("event_time", DataTypes.LongType, false)
                .add("val", DataTypes.StringType, true);
    }

    private static List<String> rows(Dataset<Row> df) {
        return df.collectAsList().stream()
                .map(r -> r.getLong(r.fieldIndex("id")) + "|"
                        + r.getLong(r.fieldIndex("event_time")) + "|"
                        + r.getString(r.fieldIndex("val")))
                .sorted()
                .collect(Collectors.toList());
    }

    private static PartitionId registerPartition(String name, long lo, long hi) {
        PartitionId id = new PartitionId(table, name);
        catalog.upsertPartition(id, new PartitionBounds(new TierKey(lo), new TierKey(hi)),
                PartitionState.HOT);
        return id;
    }

    private static long relOid(String qualified) {
        return Long.parseLong(queryOne("SELECT '" + qualified + "'::regclass::oid::bigint::text"));
    }

    private static void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String queryOne(String sql) {
        try (Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
