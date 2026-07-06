package io.modak.trino;

import io.modak.common.RowBatchData.Column;
import io.modak.connector.ConnectorConfig;
import io.modak.connector.source.HeapCatalog;
import io.modak.connector.seam.SeamClient;
import io.modak.connector.seam.SeamOptions;
import io.modak.connector.seam.SeamState;
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

public class ModakMetadata implements ConnectorMetadata {

    private final ModakConnector connector;

    ModakMetadata(ModakConnector connector) {
        this.connector = connector;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        List<String> schemas = new ArrayList<>();
        query("SELECT DISTINCT schema_name FROM modak.tables ORDER BY 1", List.of(),
                rs -> schemas.add(rs.getString(1)));
        return schemas;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        List<SchemaTableName> tables = new ArrayList<>();
        String sql = "SELECT schema_name, table_name FROM modak.tables"
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
        boolean heapComplete = state.heapIsComplete();
        if (!heapComplete && state.snapshotId() != null
                && !"iceberg".equals(state.lakeFormat())) {
            throw new TrinoException(NOT_SUPPORTED, "the modak connector cannot read lake format '"
                    + state.lakeFormat() + "' yet (supported: iceberg)");
        }
        return new ModakTableHandle(tableName.getSchemaName(), tableName.getTableName(),
                state.tableId(), state.primaryKeyCols(), state.tierKeyCol(), state.tierKeyType(),
                state.readSeam(), heapComplete,
                heapComplete ? null : state.snapshotId(),
                heapComplete ? null : state.metadataLocation(),
                TupleDomain.all());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
            ConnectorTableHandle table) {
        ModakTableHandle handle = (ModakTableHandle) table;
        List<ColumnMetadata> columns = columnHandles(handle).values().stream()
                .map(c -> new ColumnMetadata(((ModakColumnHandle) c).name(),
                        ((ModakColumnHandle) c).type()))
                .toList();
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return columnHandles((ModakTableHandle) tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
            ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        ModakColumnHandle column = (ModakColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        ModakTableHandle handle = (ModakTableHandle) table;
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

    private Map<String, ColumnHandle> columnHandles(ModakTableHandle handle) {
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
            columns.put(column.name(), new ModakColumnHandle(
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
