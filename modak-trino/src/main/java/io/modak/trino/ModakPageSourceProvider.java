package io.modak.trino;

import io.modak.connector.ConnectorConfig;
import io.modak.trino.source.ColdPageSource;
import io.modak.trino.source.DeltaUpsertPageSource;
import io.modak.trino.source.HotPageSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;

public class ModakPageSourceProvider implements ConnectorPageSourceProvider {

    private final ConnectorConfig config;

    ModakPageSourceProvider(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
            ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        ModakTableHandle handle = (ModakTableHandle) table;
        List<ModakColumnHandle> modakColumns = columns.stream()
                .map(ModakColumnHandle.class::cast)
                .toList();
        return switch (((ModakSplit) split).kind()) {
            case HOT -> new HotPageSource(config, handle, modakColumns);
            case COLD -> new ColdPageSource(config, handle, modakColumns);
            case DELTA_UPSERTS -> new DeltaUpsertPageSource(config, handle, modakColumns);
        };
    }
}
