package io.tierdb.trino.source;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import io.tierdb.connector.ConnectorConfig;
import io.tierdb.connector.source.DeltaClient;
import io.tierdb.trino.TierDBColumnHandle;
import io.tierdb.trino.TierDBTableHandle;
import io.trino.spi.TrinoException;
import java.util.Iterator;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class DeltaUpsertPageSource extends RowPageSource {

    private final ConnectorConfig config;
    private final TierDBTableHandle handle;
    private final List<TierDBColumnHandle> columns;

    public DeltaUpsertPageSource(ConnectorConfig config, TierDBTableHandle handle,
            List<TierDBColumnHandle> columns) {
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
