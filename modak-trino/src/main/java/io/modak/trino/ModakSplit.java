package io.modak.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

/**
 * One branch of the seam plan: the hot heap scan, the pinned cold scan,
 * or the delta op=0 payload overlay.
 */
public record ModakSplit(@JsonProperty("kind") Kind kind) implements ConnectorSplit {

    public enum Kind { HOT, COLD, DELTA_UPSERTS }

    @JsonCreator
    public ModakSplit {}
}
