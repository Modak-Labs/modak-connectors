package io.tierdb.trino;

import io.tierdb.connector.ConnectorConfig;
import io.tierdb.trino.source.ColdPageSource;
import io.tierdb.trino.source.DeltaUpsertPageSource;
import io.tierdb.trino.source.HotPageSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;

public class TierDBPageSourceProvider implements ConnectorPageSourceProvider {

    private final ConnectorConfig config;

    TierDBPageSourceProvider(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
            ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        TierDBTableHandle handle = (TierDBTableHandle) table;
        List<TierDBColumnHandle> tierdbColumns = columns.stream()
                .map(TierDBColumnHandle.class::cast)
                .toList();
        return switch (((TierDBSplit) split).kind()) {
            case HOT -> new HotPageSource(config, handle, tierdbColumns);
            case COLD -> new ColdPageSource(config, handle, tierdbColumns);
            case DELTA_UPSERTS -> new DeltaUpsertPageSource(config, handle, tierdbColumns);
        };
    }
}
