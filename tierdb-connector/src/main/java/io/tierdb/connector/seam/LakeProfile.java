package io.tierdb.connector.seam;

import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeAccessPlugin;
import java.util.HashMap;
import java.util.Map;

/**
 * How to reach a table's lake data: the format and table ref that identify it,
 * plus the storage profile (warehouse and provider config) needed to open it.
 */
public record LakeProfile(
        String format,
        String tableRef,
        String warehouse,
        Map<String, String> config) {

    /** The full config for the format plugin: profile plus engine overrides. */
    public Map<String, String> accessConfig(Map<String, String> overrides) {
        Map<String, String> out = new HashMap<>();
        if (warehouse != null && !warehouse.isBlank()) {
            out.put("warehouse", warehouse);
        }
        out.putAll(config);
        out.putAll(overrides);
        return Map.copyOf(out);
    }

    /** Opens the format's {@link LakeAccess} adapter for this profile. */
    public LakeAccess openAccess(Map<String, String> overrides) {
        return LakeAccessPlugin.load(format, accessConfig(overrides));
    }
}
