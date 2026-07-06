package io.tierdb.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.List;

/**
 * Trino entry point for the tierdb connector, a seam-protocol consumer
 * serving consistent two-tier reads over registered TierDB tables.
 */
public class TierDBPlugin implements Plugin {

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new TierDBConnectorFactory());
    }
}
