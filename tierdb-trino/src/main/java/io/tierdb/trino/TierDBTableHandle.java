package io.tierdb.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.util.List;
import java.util.Map;

/**
 * A registered TierDB table plus the seam captured for this query: the read
 * seam T, the resolved cold scan as neutral per-format props ({@code null}
 * when there are no cold rows), and whether the delta overlay must be merged
 * on top (false for direct tables read live). {@code lakeFormat} and
 * {@code lakeConfig} let workers reopen the format's access plugin.
 */
public record TierDBTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("tableId") long tableId,
        @JsonProperty("primaryKeyCols") List<String> primaryKeyCols,
        @JsonProperty("tierKeyCol") String tierKeyCol,
        @JsonProperty("tierKeyType") String tierKeyType,
        @JsonProperty("readSeam") long readSeam,
        @JsonProperty("heapComplete") boolean heapComplete,
        @JsonProperty("lakeFormat") String lakeFormat,
        @JsonProperty("lakeConfig") Map<String, String> lakeConfig,
        @JsonProperty("scanProps") Map<String, String> scanProps,
        @JsonProperty("mergeDelta") boolean mergeDelta,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
        implements ConnectorTableHandle {

    @JsonCreator
    public TierDBTableHandle {}

    public SchemaTableName schemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    public boolean hasColdScan() {
        return scanProps != null;
    }

    public TierDBTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        return new TierDBTableHandle(schemaName, tableName, tableId, primaryKeyCols,
                tierKeyCol, tierKeyType, readSeam, heapComplete, lakeFormat, lakeConfig,
                scanProps, mergeDelta, newConstraint);
    }

    @Override
    public String toString() {
        return schemaName + "." + tableName;
    }
}
