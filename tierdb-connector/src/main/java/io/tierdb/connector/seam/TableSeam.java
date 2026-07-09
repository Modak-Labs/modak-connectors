package io.tierdb.connector.seam;

import io.tierdb.common.mode.Mode;
import java.util.List;

public record TableSeam(
        long tableId,
        List<String> primaryKeyCols,
        String tierKeyCol,
        String tierKeyType,
        Mode mode) {}
