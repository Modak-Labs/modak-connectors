package io.tierdb.trino;

import io.tierdb.connector.ConnectorConfig;
import io.tierdb.connector.seam.SeamClient;
import io.tierdb.connector.seam.SeamOptions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * One catalog instance: hands out metadata, splits, and page sources, and
 * tracks the read pins each query captured so cleanupQuery releases them.
 */
public class TierDBConnector implements Connector {

    private final ConnectorConfig config;
    private final Map<String, List<Pin>> pinsByQuery = new ConcurrentHashMap<>();

    record Pin(SeamOptions options, long pinId) {}

    public TierDBConnector(ConnectorConfig config) {
        this.config = config;
    }

    ConnectorConfig config() {
        return config;
    }

    void registerPin(String queryId, SeamOptions options, long pinId) {
        pinsByQuery.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>())
                .add(new Pin(options, pinId));
    }

    void releasePins(String queryId) {
        List<Pin> pins = pinsByQuery.remove(queryId);
        if (pins == null) {
            return;
        }
        for (Pin pin : pins) {
            try {
                SeamClient.release(pin.options(), pin.pinId());
            } catch (RuntimeException e) {
            }
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
            boolean readOnly, boolean autoCommit) {
        return TierDBTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session,
            ConnectorTransactionHandle transactionHandle) {
        return new TierDBMetadata(this);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new TierDBSplitManager();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new TierDBPageSourceProvider(config);
    }
}
