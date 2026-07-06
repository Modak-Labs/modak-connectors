package io.tierdb.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum TierDBTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE
}
