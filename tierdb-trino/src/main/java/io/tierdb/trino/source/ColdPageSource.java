package io.tierdb.trino.source;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import io.tierdb.connector.ConnectorConfig;
import io.tierdb.connector.source.LakeRowSource;
import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeAccessPlugin;
import io.tierdb.lake.access.LakeScan;
import io.tierdb.trino.TierDBColumnHandle;
import io.tierdb.trino.TierDBTableHandle;
import io.tierdb.trino.predicate.ConstraintAdapter;
import io.trino.spi.TrinoException;
import java.util.Iterator;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class ColdPageSource extends RowPageSource {

    private final ConnectorConfig config;
    private final TierDBTableHandle handle;
    private final List<TierDBColumnHandle> columns;
    private LakeRowSource rowSource;

    public ColdPageSource(ConnectorConfig config, TierDBTableHandle handle, List<TierDBColumnHandle> columns) {
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
            LakeAccess access = LakeAccessPlugin.load(handle.lakeFormat(), handle.lakeConfig());
            rowSource = LakeRowSource.open(config.jdbcUrl(), config.jdbcProperties(),
                    handle.tableId(), access, new LakeScan(handle.scanProps()),
                    handle.mergeDelta(), lakeColumns, handle.primaryKeyCols(),
                    ConstraintAdapter.from(handle.constraint()));
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
