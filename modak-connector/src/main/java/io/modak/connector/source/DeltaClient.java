package io.modak.connector.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modak.common.PgValues;
import io.modak.common.RowBatchData.Column;
import io.modak.common.RowBatchData.ColumnType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public final class DeltaClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PKS_SQL = "SELECT pk FROM modak.delta WHERE table_id = ?";

    private static final String UPSERT_PAYLOADS_SQL = "SELECT payload::text FROM modak.delta"
            + " WHERE table_id = ? AND op = 0 AND payload IS NOT NULL";

    private DeltaClient() {}

    public static Set<String> pks(String jdbcUrl, Properties jdbcProperties, long tableId) {
        Set<String> pks = new HashSet<>();
        try (Connection c = DriverManager.getConnection(jdbcUrl, jdbcProperties);
                PreparedStatement ps = c.prepareStatement(PKS_SQL)) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pks.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("delta pk load failed for table_id=" + tableId, e);
        }
        return pks;
    }

    public static List<String> upsertPayloads(String jdbcUrl, Properties jdbcProperties,
            long tableId) {
        List<String> payloads = new ArrayList<>();
        try (Connection c = DriverManager.getConnection(jdbcUrl, jdbcProperties);
                PreparedStatement ps = c.prepareStatement(UPSERT_PAYLOADS_SQL)) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    payloads.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("delta payload load failed for table_id=" + tableId, e);
        }
        return payloads;
    }

    public static List<Object[]> upsertRows(String jdbcUrl, Properties jdbcProperties,
            long tableId, List<Column> columns) {
        List<Object[]> rows = new ArrayList<>();
        for (String payloadText : upsertPayloads(jdbcUrl, jdbcProperties, tableId)) {
            try {
                JsonNode payload = MAPPER.readTree(payloadText);
                Object[] row = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    row[i] = jsonValue(payload.get(column.name()), column.type());
                }
                rows.add(row);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "delta payload parse failed for table_id=" + tableId, e);
            }
        }
        return rows;
    }

    private static Object jsonValue(JsonNode node, ColumnType kind) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (kind == ColumnType.TEXT) {
            return node.isTextual() ? node.textValue() : node.toString();
        }
        return PgValues.parseText(node.asText(), kind);
    }
}
