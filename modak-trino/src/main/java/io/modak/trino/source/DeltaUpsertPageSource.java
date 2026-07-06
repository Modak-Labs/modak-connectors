package io.modak.trino.source;

import io.modak.common.RowBatchData.Column;
import io.modak.common.RowBatchData.ColumnType;
import io.modak.connector.ConnectorConfig;
import io.modak.connector.source.DeltaClient;
import io.modak.trino.ModakColumnHandle;
import io.modak.trino.ModakTableHandle;
import io.trino.spi.TrinoException;
import java.util.Iterator;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class DeltaUpsertPageSource extends RowPageSource {

    private final ConnectorConfig config;
    private final ModakTableHandle handle;
    private final List<ModakColumnHandle> columns;

    public DeltaUpsertPageSource(ConnectorConfig config, ModakTableHandle handle,
            List<ModakColumnHandle> columns) {
        super(columns);
        this.config = config;
        this.handle = handle;
        this.columns = columns;
    }

    @Override
    protected Iterator<Object[]> open() {
        List<Column> deltaColumns = columns.stream()
                .map(c -> new Column(c.name(), ColumnType.valueOf(c.kind())))
                .toList();
        try {
            return DeltaClient.upsertRows(config.jdbcUrl(), config.jdbcProperties(),
                    handle.tableId(), deltaColumns).iterator();
        } catch (IllegalStateException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "delta overlay load failed for " + handle, e);
        }
    }

    @Override
    protected void closeUnderlying() {}
}
