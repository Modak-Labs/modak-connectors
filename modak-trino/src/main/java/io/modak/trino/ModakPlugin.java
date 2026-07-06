package io.modak.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.List;

/**
 * Trino entry point for the modak connector, a seam-protocol consumer
 * serving consistent two-tier reads over registered Modak tables.
 */
public class ModakPlugin implements Plugin {

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new ModakConnectorFactory());
    }
}
