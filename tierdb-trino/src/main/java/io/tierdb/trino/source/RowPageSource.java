package io.tierdb.trino.source;

import io.tierdb.trino.TierDBColumnHandle;
import io.tierdb.trino.TrinoTypes;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import java.util.Iterator;
import java.util.List;

public abstract class RowPageSource implements ConnectorPageSource {

    private final List<TierDBColumnHandle> columns;
    private final PageBuilder pageBuilder;
    private Iterator<Object[]> rows;
    private boolean finished;
    private long readTimeNanos;

    protected RowPageSource(List<TierDBColumnHandle> columns) {
        this.columns = columns;
        this.pageBuilder = new PageBuilder(
                columns.stream().map(c -> (io.trino.spi.type.Type) c.type()).toList());
    }

    protected abstract Iterator<Object[]> open();

    protected abstract void closeUnderlying();

    @Override
    public Page getNextPage() {
        long start = System.nanoTime();
        ClassLoader pluginLoader = getClass().getClassLoader();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(pluginLoader);
        try {
            if (rows == null) {
                rows = open();
            }
            while (!pageBuilder.isFull() && rows.hasNext()) {
                Object[] row = rows.next();
                pageBuilder.declarePosition();
                for (int i = 0; i < columns.size(); i++) {
                    TrinoTypes.write(columns.get(i).type(),
                            pageBuilder.getBlockBuilder(i), row[i]);
                }
            }
            finished = !rows.hasNext();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        readTimeNanos += System.nanoTime() - start;
        if (pageBuilder.isEmpty()) {
            return null;
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return readTimeNanos;
    }

    @Override
    public long getMemoryUsage() {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close() {
        finished = true;
        closeUnderlying();
    }
}
