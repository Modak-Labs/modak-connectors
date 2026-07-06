package io.tierdb.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.util.List;

/**
 * A registered TierDB table plus the seam captured for this query: the
 * read seam T, the pinned snapshot S, and the pinned metadata location.
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
        @JsonProperty("snapshotId") Long snapshotId,
        @JsonProperty("metadataLocation") String metadataLocation,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
        implements ConnectorTableHandle {

    @JsonCreator
    public TierDBTableHandle {}

    public SchemaTableName schemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    public TierDBTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        return new TierDBTableHandle(schemaName, tableName, tableId, primaryKeyCols,
                tierKeyCol, tierKeyType, readSeam, heapComplete, snapshotId,
                metadataLocation, newConstraint);
    }

    @Override
    public String toString() {
        return schemaName + "." + tableName;
    }
}
