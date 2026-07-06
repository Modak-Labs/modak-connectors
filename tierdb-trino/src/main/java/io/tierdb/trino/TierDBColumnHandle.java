package io.tierdb.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

public record TierDBColumnHandle(
        @JsonProperty("name") String name,
        @JsonProperty("type") Type type,
        @JsonProperty("kind") String kind)
        implements ColumnHandle {

    @JsonCreator
    public TierDBColumnHandle {}

    @Override
    public String toString() {
        return name;
    }
}
