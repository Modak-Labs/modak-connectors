package io.modak.trino.source;

import io.modak.common.RowBatchData.Column;
import io.modak.common.RowBatchData.ColumnType;
import io.modak.connector.ConnectorConfig;
import io.modak.connector.seam.TierKeySql;
import io.modak.connector.source.HeapRowSource;
import io.modak.trino.ModakColumnHandle;
import io.modak.trino.ModakTableHandle;
import io.modak.trino.predicate.PushdownSql;
import io.trino.spi.TrinoException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class HotPageSource extends RowPageSource {

    private final ConnectorConfig config;
    private final ModakTableHandle handle;
    private final List<ModakColumnHandle> columns;
    private HeapRowSource rowSource;

    public HotPageSource(ConnectorConfig config, ModakTableHandle handle, List<ModakColumnHandle> columns) {
        super(columns);
        this.config = config;
        this.handle = handle;
        this.columns = columns;
    }

    @Override
    protected Iterator<Object[]> open() {
        List<Column> heapColumns = columns.stream()
                .map(c -> new Column(c.name(), ColumnType.valueOf(c.kind())))
                .toList();
        try {
            rowSource = HeapRowSource.open(config.jdbcUrl(), config.jdbcProperties(), sql(), heapColumns);
            return rowSource;
        } catch (IllegalStateException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "hot scan failed for " + handle, e);
        }
    }

    private String sql() {
        String select = columns.isEmpty() ? "1"
                : columns.stream()
                        .map(c -> PushdownSql.quoteIdent(c.name()))
                        .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder("SELECT ").append(select)
                .append(" FROM ").append(PushdownSql.quoteIdent(handle.schemaName()))
                .append(".").append(PushdownSql.quoteIdent(handle.tableName()));
        List<String> where = new ArrayList<>();
        if (!handle.heapComplete()) {
            where.add(PushdownSql.quoteIdent(handle.tierKeyCol()) + " >= "
                    + TierKeySql.literal(handle.tierKeyType(), handle.readSeam()));
        }
        PushdownSql.where(handle.constraint()).ifPresent(where::add);
        if (!where.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", where));
        }
        return sql.toString();
    }

    @Override
    protected void closeUnderlying() {
        if (rowSource != null) {
            rowSource.close();
        }
    }
}
