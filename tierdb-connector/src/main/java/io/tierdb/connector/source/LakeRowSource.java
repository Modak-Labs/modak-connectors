package io.tierdb.connector.source;

import io.tierdb.common.PkCodec;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.access.ColumnConstraint;
import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeScan;
import io.tierdb.lake.access.RowScan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

/**
 * Format-blind cold rows for a resolved scan: reads through the format's
 * {@link LakeAccess} port and, when the scan is pinned, drops rows whose PK
 * is overridden in the {@code tierdb.delta} overlay.
 */
public final class LakeRowSource implements Iterator<Object[]>, AutoCloseable {

    private final int width;
    private final int[] pkIndexes;
    private final Set<String> deltaPks;
    private final RowScan rows;
    private Object[] next;

    private LakeRowSource(int width, int[] pkIndexes, Set<String> deltaPks, RowScan rows) {
        this.width = width;
        this.pkIndexes = pkIndexes;
        this.deltaPks = deltaPks;
        this.rows = rows;
    }

    public static LakeRowSource open(String jdbcUrl, Properties jdbcProperties, long tableId,
            LakeAccess access, LakeScan scan, boolean mergeDelta, List<Column> columns,
            List<String> primaryKeyCols, Map<Column, ColumnConstraint> filter) {
        Set<String> deltaPks = mergeDelta
                ? DeltaClient.pks(jdbcUrl, jdbcProperties, tableId)
                : Set.of();

        LinkedHashSet<String> selected = new LinkedHashSet<>();
        columns.forEach(c -> selected.add(c.name()));
        selected.addAll(primaryKeyCols);
        List<String> selectedList = List.copyOf(selected);

        int[] pkIndexes = new int[primaryKeyCols.size()];
        for (int i = 0; i < primaryKeyCols.size(); i++) {
            pkIndexes[i] = selectedList.indexOf(primaryKeyCols.get(i));
        }

        return new LakeRowSource(columns.size(), pkIndexes, deltaPks,
                access.rows(scan, selectedList, filter));
    }

    @Override
    public boolean hasNext() {
        while (next == null && rows.hasNext()) {
            Object[] row = rows.next();
            if (!deltaPks.isEmpty() && deltaPks.contains(pkOf(row))) {
                continue;
            }
            // Requested columns lead the selection; trim off the PK-only tail.
            next = row.length == width ? row : Arrays.copyOf(row, width);
        }
        return next != null;
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Object[] row = next;
        next = null;
        return row;
    }

    @Override
    public void close() {
        rows.close();
    }

    private String pkOf(Object[] row) {
        if (pkIndexes.length == 1) {
            return String.valueOf(row[pkIndexes[0]]);
        }
        List<String> parts = new ArrayList<>(pkIndexes.length);
        for (int index : pkIndexes) {
            parts.add(String.valueOf(row[index]));
        }
        return PkCodec.encode(parts);
    }
}
