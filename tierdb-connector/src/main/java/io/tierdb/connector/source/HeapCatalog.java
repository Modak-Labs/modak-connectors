package io.tierdb.connector.source;

import io.tierdb.common.PgValues;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public final class HeapCatalog {

    private static final String COLUMNS_SQL = """
            SELECT column_name, udt_name,
                   coalesce(numeric_precision, 0), coalesce(numeric_scale, 0)
              FROM information_schema.columns
             WHERE table_schema = ? AND table_name = ?
             ORDER BY ordinal_position
            """;

    private static final Set<String> TEXT_TYPES = Set.of(
            "text", "varchar", "character varying", "char", "character",
            "bpchar", "name", "citext", "json", "jsonb");

    private HeapCatalog() {}

    public static List<Column> columns(String jdbcUrl, Properties jdbcProperties,
            String schema, String table) {
        List<Column> columns = new ArrayList<>();
        try (Connection c = DriverManager.getConnection(jdbcUrl, jdbcProperties);
                PreparedStatement ps = c.prepareStatement(COLUMNS_SQL)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String pgTypeName = rs.getString(2);
                    Column column = PgValues.column(name, pgTypeName, rs.getInt(3), rs.getInt(4));
                    if (column.type() == ColumnType.TEXT && !TEXT_TYPES.contains(pgTypeName)) {
                        throw new IllegalArgumentException(
                                "column " + name + " has type " + pgTypeName + " which is not supported");
                    }
                    columns.add(column);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "schema discovery failed for " + schema + "." + table, e);
        }
        return columns;
    }
}
