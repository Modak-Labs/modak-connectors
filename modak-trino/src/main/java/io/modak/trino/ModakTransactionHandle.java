package io.modak.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum ModakTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE
}
