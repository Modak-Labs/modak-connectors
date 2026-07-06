package io.tierdb.connector.source;

import io.tierdb.common.PkCodec;
import io.tierdb.common.RowBatchData.Column;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

public final class LakeRowSource implements Iterator<Object[]>, AutoCloseable {

    private final List<Column> columns;
    private final List<String> primaryKeyCols;
    private final Set<String> deltaPks;
    private final FileIO fileIo;
    private final CloseableIterable<Record> iterable;
    private final Iterator<Record> records;
    private Object[] next;

    private LakeRowSource(List<Column> columns, List<String> primaryKeyCols, Set<String> deltaPks,
            FileIO fileIo, CloseableIterable<Record> iterable) {
        this.columns = columns;
        this.primaryKeyCols = primaryKeyCols;
        this.deltaPks = deltaPks;
        this.fileIo = fileIo;
        this.iterable = iterable;
        this.records = iterable.iterator();
    }

    public static LakeRowSource open(String jdbcUrl, Properties jdbcProperties, long tableId,
            String metadataLocation, long snapshotId, String displayName, List<Column> columns,
            List<String> primaryKeyCols, Map<String, String> fileIoProperties,
            Optional<Expression> filter) {
        Set<String> deltaPks = DeltaClient.pks(jdbcUrl, jdbcProperties, tableId);
        FileIO fileIo = LakeTables.fileIo(fileIoProperties);
        Table table = new BaseTable(
                new StaticTableOperations(metadataLocation, fileIo), displayName);

        Set<String> selected = new LinkedHashSet<>();
        columns.forEach(c -> selected.add(c.name()));
        selected.addAll(primaryKeyCols);

        IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(table)
                .useSnapshot(snapshotId)
                .select(selected.toArray(String[]::new));
        if (filter.isPresent()) {
            scan = scan.where(filter.get());
        }
        return new LakeRowSource(columns, primaryKeyCols, deltaPks, fileIo, scan.build());
    }

    @Override
    public boolean hasNext() {
        while (next == null && records.hasNext()) {
            Record record = records.next();
            if (deltaPks.contains(pkOf(record))) {
                continue;
            }
            Object[] row = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                row[i] = normalize(record.getField(columns.get(i).name()));
            }
            next = row;
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
        try {
            iterable.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            fileIo.close();
        }
    }

    private String pkOf(Record record) {
        if (primaryKeyCols.size() == 1) {
            return String.valueOf(record.getField(primaryKeyCols.get(0)));
        }
        List<String> parts = new ArrayList<>(primaryKeyCols.size());
        for (String col : primaryKeyCols) {
            parts.add(String.valueOf(record.getField(col)));
        }
        return PkCodec.encode(parts);
    }

    private static Object normalize(Object value) {
        if (value instanceof LocalDateTime ldt) {
            return ldt.atOffset(ZoneOffset.UTC);
        }
        if (value instanceof ByteBuffer buf) {
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return bytes;
        }
        return value;
    }
}
