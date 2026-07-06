package io.modak.connector.source;

import io.modak.common.PgValues;
import io.modak.common.RowBatchData.Column;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

public final class HeapRowSource implements Iterator<Object[]>, AutoCloseable {

    private final List<Column> columns;
    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private Object[] next;
    private boolean done;

    private HeapRowSource(List<Column> columns, Connection connection, Statement statement,
            ResultSet resultSet) {
        this.columns = columns;
        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
    }

    public static HeapRowSource open(String jdbcUrl, Properties jdbcProperties, String sql,
            List<Column> columns) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl, jdbcProperties);
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            statement.setFetchSize(1024);
            ResultSet resultSet = statement.executeQuery(sql);
            return new HeapRowSource(columns, connection, statement, resultSet);
        } catch (SQLException e) {
            closeQuietly(statement, connection);
            throw new IllegalStateException("heap scan failed", e);
        }
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        if (done) {
            return false;
        }
        try {
            if (!resultSet.next()) {
                done = true;
                return false;
            }
            Object[] row = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                row[i] = PgValues.readValue(resultSet, i + 1, columns.get(i).type());
            }
            next = row;
            return true;
        } catch (SQLException e) {
            throw new IllegalStateException("heap scan failed", e);
        }
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
        closeQuietly(statement, connection);
    }

    private static void closeQuietly(Statement statement, Connection connection) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ignored) {
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignored) {
        }
    }
}
