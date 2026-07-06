package io.tierdb.connector.predicate;

import io.tierdb.common.RowBatchData.Column;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

public final class SqlPredicates implements PredicateRenderer<String> {

    public static final SqlPredicates INSTANCE = new SqlPredicates();

    private SqlPredicates() {}

    public static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public static Optional<String> where(Map<Column, ColumnConstraint> constraints) {
        return ConstraintRenderer.render(constraints, INSTANCE);
    }

    @Override
    public String alwaysTrue() {
        return "TRUE";
    }

    @Override
    public String alwaysFalse() {
        return "FALSE";
    }

    @Override
    public String isNull(String column) {
        return quoteIdent(column) + " IS NULL";
    }

    @Override
    public String equal(String column, Object value) {
        return quoteIdent(column) + " = " + literal(value);
    }

    @Override
    public String greaterThanOrEqual(String column, Object value) {
        return quoteIdent(column) + " >= " + literal(value);
    }

    @Override
    public String greaterThan(String column, Object value) {
        return quoteIdent(column) + " > " + literal(value);
    }

    @Override
    public String lessThanOrEqual(String column, Object value) {
        return quoteIdent(column) + " <= " + literal(value);
    }

    @Override
    public String lessThan(String column, Object value) {
        return quoteIdent(column) + " < " + literal(value);
    }

    @Override
    public String and(String left, String right) {
        return "(" + left + " AND " + right + ")";
    }

    @Override
    public String or(String left, String right) {
        return "(" + left + " OR " + right + ")";
    }

    private static String literal(Object value) {
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        if (value instanceof Long) {
            return Long.toString((Long) value);
        }
        if (value instanceof Double) {
            return Double.toString((Double) value);
        }
        if (value instanceof LocalDate) {
            return "DATE '" + value + "'";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        throw new IllegalArgumentException("unsupported literal: " + value);
    }
}
