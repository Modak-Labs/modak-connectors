package io.tierdb.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;
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
import io.tierdb.lake.iceberg.IcebergLakeStoragePlugin;
import io.tierdb.tiering.JdbcHotSource;
import io.tierdb.tiering.TieringWorker;
import io.tierdb.tiering.policy.SealGatedEvictionPolicy;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * The worker tiers a real partitioned table into Iceberg, delta rows correct
 * cold data, and Trino serves one consistent view through the tierdb catalog.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TierDBTrinoSeamTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static TableId table;
    private static String location;
    private static DistributedQueryRunner runner;

    @BeforeAll
    static void setUpWorld() throws Exception {
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
                  (1, 10, 'a'), (2, 20, 'b'),
                  (3, 110, 'c'),
                  (4, 210, 'hot'), (5, 220, 'hot')
                """);

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.required(2, "event_time",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(3, "val",
                        org.apache.iceberg.types.Types.StringType.get()));
        location = warehouse.resolve("events_cold").toString();
        new HadoopTables(new Configuration())
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

        Session session = testSessionBuilder()
                .setCatalog("tierdb")
                .setSchema("public")
                .build();
        runner = DistributedQueryRunner.builder(session).build();
        runner.installPlugin(new TierDBPlugin());
        runner.createCatalog("tierdb", "tierdb",
                Map.of("tierdb.jdbc-url", postgres.getJdbcUrl("postgres", "postgres")));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (runner != null) {
            runner.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    @Order(1)
    void seamReadMergesHotColdAndDelta() {
        assertEquals(List.of("1|10|a", "2|20|b2", "4|210|hot", "5|220|hot"),
                rows("SELECT id, event_time, val FROM events"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.read_pins"),
                "the pin is released when the query finishes");
    }

    @Test
    @Order(2)
    void readsThePinnedMetadataNotTheLiveTable() throws Exception {
        Table iceberg = new HadoopTables(new Configuration()).load(location);
        GenericRecord late = GenericRecord.create(iceberg.schema());
        late.setField("id", 99L);
        late.setField("event_time", 50L);
        late.setField("val", "late");
        GenericAppenderFactory appenders = new GenericAppenderFactory(iceberg.schema());
        DataWriter<Record> writer = appenders.newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(
                        iceberg.io().newOutputFile(location + "/data/late.parquet")),
                FileFormat.PARQUET, null);
        writer.write(late);
        writer.close();
        iceberg.newAppend().appendFile(writer.toDataFile()).commit();

        assertEquals(4L, runner.execute("SELECT count(*) FROM events")
                .getOnlyValue(), "snapshots committed after the cut-line stay invisible");
    }

    @Test
    @Order(3)
    void predicatePushdownStaysCorrectAcrossBranches() {
        assertEquals(List.of("4", "5"), rows("SELECT id FROM events WHERE event_time >= 210"));
        assertEquals(List.of("1"), rows("SELECT id FROM events WHERE val = 'a'"));
        assertEquals(List.of("2|b2"), rows("SELECT id, val FROM events WHERE id = 2"),
                "the delta correction wins even under a pushed-down filter");
        assertEquals(List.of(), rows("SELECT id FROM events WHERE id = 3"),
                "the delta tombstone hides the cold row");
    }

    @Test
    @Order(4)
    void projectionsAndAggregatesReadSubsets() {
        assertEquals(List.of("a", "b2", "hot", "hot"), rows("SELECT val FROM events"));
        assertEquals(4L, runner.execute("SELECT count(*) FROM events").getOnlyValue());
    }

    @Test
    @Order(5)
    void mirroredTablesReadAsAPlainHeap() {
        exec("CREATE TABLE public.readings (id bigint NOT NULL, event_time bigint NOT NULL, val text)");
        exec("INSERT INTO public.readings VALUES (1, 5, 'm1'), (2, 1500, 'm2')");
        TableId mirrored = catalog.register(new TableRegistration(
                relOid("public.readings"), "public", "readings", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER,
                warehouse.resolve("readings_mirror").toString(),
                TableMode.MIRRORED, "pub_readings", "slot_readings",
                Optional.empty(), Optional.empty()));
        catalog.initCutline(mirrored, new TierKey(Long.MIN_VALUE), new LakeSnapshotId(0));

        assertEquals(List.of("1|5|m1", "2|1500|m2"),
                rows("SELECT id, event_time, val FROM readings"));
    }

    @Test
    @Order(6)
    void unsupportedColumnTypesFailLoudly() {
        exec("CREATE TABLE public.leases (id bigint NOT NULL, span interval, event_time bigint NOT NULL)");
        TableId leases = catalog.register(new TableRegistration(
                relOid("public.leases"), "public", "leases", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER,
                warehouse.resolve("leases_mirror").toString(),
                TableMode.MIRRORED, "pub_leases", "slot_leases",
                Optional.empty(), Optional.empty()));
        catalog.initCutline(leases, new TierKey(Long.MIN_VALUE), new LakeSnapshotId(0));

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> runner.execute("SELECT * FROM leases"));
        assertTrue(e.getMessage().contains("not supported"), e.getMessage());
    }

    @Test
    @Order(7)
    void directTablesReadTheLiveLakeWithoutTheDelta() throws Exception {
        exec("CREATE TABLE public.metrics (id bigint NOT NULL, event_time bigint NOT NULL, val text)");
        exec("INSERT INTO public.metrics VALUES (1, 150, 'd-hot')");

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.required(2, "event_time",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(3, "val",
                        org.apache.iceberg.types.Types.StringType.get()));
        String directLocation = warehouse.resolve("metrics_direct").toString();
        Table iceberg = new HadoopTables(new Configuration())
                .create(schema, org.apache.iceberg.PartitionSpec.unpartitioned(), directLocation);

        TableId direct = catalog.register(new TableRegistration(
                relOid("public.metrics"), "public", "metrics", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER, directLocation,
                TableMode.DIRECT, null, null, Optional.empty(), Optional.empty()));
        catalog.initCutline(direct, new TierKey(100), new LakeSnapshotId(0));

        assertEquals(List.of("1|150|d-hot"),
                rows("SELECT id, event_time, val FROM metrics"),
                "an empty lake reads hot-only");

        appendDirect(iceberg, 2L, 50L, "d-cold");
        assertEquals(List.of("1|150|d-hot", "2|50|d-cold"),
                rows("SELECT id, event_time, val FROM metrics"),
                "the lake commit is immediately visible without a cutline publish");

        exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES ("
                + direct.oid() + ", '2', 1, 50, nextval('tierdb.delta_version'), NULL)");
        assertEquals(List.of("1|150|d-hot", "2|50|d-cold"),
                rows("SELECT id, event_time, val FROM metrics"),
                "the delta overlay is never merged for direct tables");
    }

    private static void appendDirect(Table iceberg, long id, long eventTime, String val)
            throws Exception {
        GenericRecord row = GenericRecord.create(iceberg.schema());
        row.setField("id", id);
        row.setField("event_time", eventTime);
        row.setField("val", val);
        GenericAppenderFactory appenders = new GenericAppenderFactory(iceberg.schema());
        DataWriter<Record> writer = appenders.newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(iceberg.io().newOutputFile(
                        iceberg.location() + "/data/direct-" + id + ".parquet")),
                FileFormat.PARQUET, null);
        writer.write(row);
        writer.close();
        iceberg.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private static List<String> rows(String sql) {
        MaterializedResult result = runner.execute(sql);
        return result.getMaterializedRows().stream()
                .map(TierDBTrinoSeamTest::formatRow)
                .sorted()
                .collect(Collectors.toList());
    }

    private static String formatRow(MaterializedRow row) {
        return row.getFields().stream()
                .map(String::valueOf)
                .collect(Collectors.joining("|"));
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
