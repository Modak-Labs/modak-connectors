package io.modak.trino.source;

import io.modak.common.RowBatchData.Column;
import io.modak.common.RowBatchData.ColumnType;
import io.modak.connector.ConnectorConfig;
import io.modak.connector.source.LakeRowSource;
import io.modak.trino.ModakColumnHandle;
import io.modak.trino.ModakTableHandle;
import io.modak.trino.predicate.IcebergFilters;
import io.trino.spi.TrinoException;
import java.util.Iterator;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class ColdPageSource extends RowPageSource {

    private final ConnectorConfig config;
    private final ModakTableHandle handle;
    private final List<ModakColumnHandle> columns;
    private LakeRowSource rowSource;

    public ColdPageSource(ConnectorConfig config, ModakTableHandle handle, List<ModakColumnHandle> columns) {
        super(columns);
        this.config = config;
        this.handle = handle;
        this.columns = columns;
    }

    @Override
    protected Iterator<Object[]> open() {
        List<Column> lakeColumns = columns.stream()
                .map(c -> new Column(c.name(), ColumnType.valueOf(c.kind())))
                .toList();
        try {
            rowSource = LakeRowSource.open(config.jdbcUrl(), config.jdbcProperties(),
                    handle.tableId(), handle.metadataLocation(), handle.snapshotId(),
                    handle.schemaName() + "." + handle.tableName(), lakeColumns,
                    handle.primaryKeyCols(), config.fileIoProperties(),
                    IcebergFilters.expression(handle.constraint()));
        } catch (IllegalStateException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "cold scan failed for " + handle, e);
        }
        return rowSource;
    }

    @Override
    protected void closeUnderlying() {
        if (rowSource != null) {
            rowSource.close();
        }
    }
}
