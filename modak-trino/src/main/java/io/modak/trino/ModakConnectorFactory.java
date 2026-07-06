package io.modak.trino;

import io.modak.connector.ConnectorConfig;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class ModakConnectorFactory implements ConnectorFactory {

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public String getName() {
        return "modak";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config,
            ConnectorContext context) {
        return new ModakConnector(ConnectorConfig.fromMap(config));
    }
}
