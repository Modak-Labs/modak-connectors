package io.tierdb.connector.seam;

import java.util.Map;

/**
 * The captured seam position: the tier cut (T) and the lake props the cut-line
 * published for the pinned snapshot (S), opaque to the seam layer. Direct
 * tables ignore the pinned props and resolve the lake live at read time.
 */
public record CutLine(
        long tierKeyHi,
        Long retentionLine,
        Long hybridSeam,
        Map<String, String> lakeProps) {

    public long readSeam() {
        return hybridSeam != null ? hybridSeam : tierKeyHi;
    }
}
