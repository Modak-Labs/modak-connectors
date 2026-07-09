package io.tierdb.trino;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.connector.ConnectorConfig;
import io.tierdb.connector.read.Cold;
import io.tierdb.connector.read.Read;
import io.tierdb.connector.seam.SeamClient;
import io.tierdb.connector.seam.SeamOptions;
import io.tierdb.connector.seam.SeamState;
import io.tierdb.connector.source.HeapCatalog;
import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeScan;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class TierDBMetadata implements ConnectorMetadata {

    private final TierDBConnector connector;

    TierDBMetadata(TierDBConnector connector) {
        this.connector = connector;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        List<String> schemas = new ArrayList<>();
        query("SELECT DISTINCT schema_name FROM tierdb.tables ORDER BY 1", List.of(),
                rs -> schemas.add(rs.getString(1)));
        return schemas;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        List<SchemaTableName> tables = new ArrayList<>();
        String sql = "SELECT schema_name, table_name FROM tierdb.tables"
                + (schemaName.isPresent() ? " WHERE schema_name = ?" : "") + " ORDER BY 1, 2";
        query(sql, schemaName.map(List::of).orElse(List.of()),
                rs -> tables.add(new SchemaTableName(rs.getString(1), rs.getString(2))));
        return tables;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "versioned reads are not supported");
        }
        SeamOptions options = connector.config()
                .seamOptions(tableName.getSchemaName() + "." + tableName.getTableName());
        SeamState state;
        try {
            state = SeamClient.capture(options, true);
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().contains("not registered")) {
                return null;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "seam capture failed for " + tableName, e);
        }
        if (state.pinId() != null) {
            connector.registerPin(session.getQueryId(), options, state.pinId());
        }
        Read read = resolveRead(state);
        boolean heapComplete = read instanceof Read.Heap;
        LakeScan scan = coldScan(read);
        boolean mergeDelta = read instanceof Read.Seam s && s.cold() instanceof Cold.Merge;
        Map<String, String> lakeConfig = scan == null ? null
                : state.lake().accessConfig(connector.config().fileIoProperties());
        return new TierDBTableHandle(tableName.getSchemaName(), tableName.getTableName(),
                state.table().tableId(), state.table().primaryKeyCols(),
                state.table().tierKeyCol(), state.table().tierKeyType(),
                state.readSeam(), heapComplete,
                state.lake().format(), lakeConfig,
                scan == null ? null : scan.props(),
                mergeDelta,
                TupleDomain.all());
    }

    private Read resolveRead(SeamState state) {
        boolean hybrid = state.cutLine().hybridSeam() != null;
        if (!hybrid && !state.mode().isDirect() && state.cutLine().lakeProps().isEmpty()) {
            return state.mode().heapComplete() ? new Read.Heap()
                    : new Read.Seam(state.readSeam(), new Cold.Delta());
        }
        LakeAccess access;
        try {
            access = state.lake().openAccess(connector.config().fileIoProperties());
        } catch (IllegalStateException e) {
            throw new TrinoException(NOT_SUPPORTED,
                    "the tierdb connector cannot read lake format '"
                            + state.lake().format() + "': " + e.getMessage(), e);
        }
        return hybrid ? state.scanHybrid(access) : state.scan(access);
    }

    private static LakeScan coldScan(Read read) {
        if (!(read instanceof Read.Seam seam)) {
            return null;
        }
        Cold cold = seam.cold();
        if (cold instanceof Cold.Live live) {
            return live.scan();
        }
        if (cold instanceof Cold.Merge merge) {
            return merge.scan();
        }
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
            ConnectorTableHandle table) {
        TierDBTableHandle handle = (TierDBTableHandle) table;
        List<ColumnMetadata> columns = columnHandles(handle).values().stream()
                .map(c -> new ColumnMetadata(((TierDBColumnHandle) c).name(),
                        ((TierDBColumnHandle) c).type()))
                .toList();
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return columnHandles((TierDBTableHandle) tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
            ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        TierDBColumnHandle column = (TierDBColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        TierDBTableHandle handle = (TierDBTableHandle) table;
        TupleDomain<ColumnHandle> combined = handle.constraint()
                .intersect(constraint.getSummary());
        if (combined.equals(handle.constraint())) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                handle.withConstraint(combined), constraint.getSummary(),
                constraint.getExpression(), false));
    }

    @Override
    public void cleanupQuery(ConnectorSession session) {
        connector.releasePins(session.getQueryId());
    }

    private Map<String, ColumnHandle> columnHandles(TierDBTableHandle handle) {
        ConnectorConfig config = connector.config();
        List<Column> discovered;
        try {
            discovered = HeapCatalog.columns(config.jdbcUrl(), config.jdbcProperties(),
                    handle.schemaName(), handle.tableName());
        } catch (IllegalStateException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "catalog query failed", e);
        } catch (IllegalArgumentException e) {
            throw new TrinoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        if (discovered.isEmpty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "no columns found for " + handle.schemaTableName());
        }
        Map<String, ColumnHandle> columns = new LinkedHashMap<>();
        for (Column column : discovered) {
            columns.put(column.name(), new TierDBColumnHandle(
                    column.name(), TrinoTypes.fromPg(column), column.type().name()));
        }
        return columns;
    }

    private interface RowConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    private void query(String sql, List<String> params, RowConsumer consumer) {
        ConnectorConfig config = connector.config();
        try (Connection c = DriverManager.getConnection(config.jdbcUrl(), config.jdbcProperties());
                PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setString(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    consumer.accept(rs);
                }
            }
        } catch (SQLException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "catalog query failed", e);
        }
    }
}
