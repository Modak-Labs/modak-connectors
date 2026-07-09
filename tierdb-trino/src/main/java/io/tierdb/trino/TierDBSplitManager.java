package io.tierdb.trino;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import java.util.ArrayList;
import java.util.List;

public class TierDBSplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
            ConnectorSession session, ConnectorTableHandle table,
            DynamicFilter dynamicFilter, Constraint constraint) {
        TierDBTableHandle handle = (TierDBTableHandle) table;
        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new TierDBSplit(TierDBSplit.Kind.HOT));
        if (!handle.heapComplete() && handle.hasColdScan()) {
            splits.add(new TierDBSplit(TierDBSplit.Kind.COLD));
            if (handle.mergeDelta()) {
                splits.add(new TierDBSplit(TierDBSplit.Kind.DELTA_UPSERTS));
            }
        }
        return new FixedSplitSource(splits);
    }
}
