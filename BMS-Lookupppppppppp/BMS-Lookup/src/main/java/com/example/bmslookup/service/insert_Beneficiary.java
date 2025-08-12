package com.example.bmslookup.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.util.*;

@Service
public class insert_Beneficiary {

    // Helper to quote columns with special characters
    private String quoteColumn(String col) {
        if (col.matches(".*[ ()].*")) { // contains space or parenthesis
            return "\"" + col + "\"";
        }
        return col;
    }

    // Mapping from JSON keys to DB column names
    private static final Map<String, String> FIELD_NAME_MAP;
    static {
        FIELD_NAME_MAP = new HashMap<>();
        FIELD_NAME_MAP.put("LandlineNumber(Work)", "LandlineNumber(Work)");
        // Add more mappings as needed
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Cache for table columns
    private final Map<String, Set<String>> tableColumnsCache = new HashMap<>();

    // Get columns from DB (stored in lowercase for case-insensitive match)
    private Set<String> getTableColumns(String tableName) {
        if (tableColumnsCache.containsKey(tableName)) {
            return tableColumnsCache.get(tableName);
        }
        Set<String> columns = new HashSet<>();
        try {
            java.sql.Connection conn = jdbcTemplate.getDataSource().getConnection();
            try {
                java.sql.DatabaseMetaData meta = conn.getMetaData();
                String schema = null;
                String table = tableName;
                if (tableName.contains(".")) {
                    String[] parts = tableName.split("\\.");
                    schema = parts[0];
                    table = parts[1];
                }
                try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME").toLowerCase()); // ✅ lowercase
                    }
                }
            } finally {
                conn.close();
            }
        } catch (Exception e) {
            // fallback: empty set
        }
        tableColumnsCache.put(tableName, columns);
        return columns;
    }

    // Check if record exists
    private boolean exists(String table, String id) {
        String sql = "SELECT COUNT(*) FROM " + table + " WHERE id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, new Object[]{id}, Integer.class);
        return count != null && count > 0;
    }

    public void insertBeneficiaryData(String jsonPayload) throws Exception {
        JsonNode root = objectMapper.readTree(jsonPayload);
        JsonNode enrollmentData = root.get("enrollmentData");
        if (enrollmentData == null) throw new Exception("enrollmentData node missing");
        JsonNode beneficiaries = enrollmentData.get("beneficiaryData");
        if (beneficiaries == null || !beneficiaries.isArray()) throw new Exception("beneficiaryData array missing");

        for (JsonNode beneficiary : beneficiaries) {
            insertIfNotExists("GDEV1T_UHI_DATA.beneficiary", beneficiary, null);
            String parentBeneficiaryId = beneficiary.has("id") ? beneficiary.get("id").asText() : null;
            if (beneficiary.has("employments") && beneficiary.get("employments").isArray()) {
                for (JsonNode employment : beneficiary.get("employments")) {
                    insertIfNotExists("GDEV1T_UHI_DATA.employment", employment, parentBeneficiaryId);
                }
            }
            if (beneficiary.has("contacts") && beneficiary.get("contacts").isArray()) {
                for (JsonNode contact : beneficiary.get("contacts")) {
                    insertIfNotExists("GDEV1T_UHI_DATA.contact", contact, parentBeneficiaryId);
                }
            }
        }
    }

    // Insert Only (no update)
    private void insertIfNotExists(String table, JsonNode data, String parentBeneficiaryId) {
        String id = data.has("id") ? data.get("id").asText() : null;
        if (id == null) return;
        boolean exists;
        if ("GDEV1T_UHI_DATA.beneficiary".equalsIgnoreCase(table)) {
            exists = exists(table, id);
        } else {
            // For employment/contact: check by (id, beneficiaryId or beneficiaryID)
            String bId = data.has("beneficiaryId") ? data.get("beneficiaryId").asText() :
                         (data.has("beneficiaryID") ? data.get("beneficiaryID").asText() : parentBeneficiaryId);
            exists = existsSub(table, id, bId);
        }
        if (!exists) {
            dynamicInsert(table, data);
        }
    }

    // For employment/contact: check by (id, beneficiaryId)
    private boolean existsSub(String table, String id, String beneficiaryId) {
        String sql = "SELECT COUNT(*) FROM " + table + " WHERE id = ? AND (beneficiaryId = ? OR beneficiaryID = ?)";
        Integer count = jdbcTemplate.queryForObject(sql, new Object[]{id, beneficiaryId, beneficiaryId}, Integer.class);
        return count != null && count > 0;
    }

    // Insert
    private void dynamicInsert(String table, JsonNode data) {
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        List<String> columns = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        Set<String> validColumns = getTableColumns(table);

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();

            // ✅ Convert JSON key to lowercase for comparison
            String col = FIELD_NAME_MAP.getOrDefault(entry.getKey(), entry.getKey()).toLowerCase();
            JsonNode v = entry.getValue();

            if (v.isContainerNode()) continue;
            if (!validColumns.contains(col)) continue;

            columns.add(quoteColumn(col)); // col is already lowercase
            placeholders.add("?");
            if (v.isNull()) values.add(null);
            else if (v.isNumber()) values.add(v.numberValue());
            else if (v.isBoolean()) values.add(v.booleanValue());
            else values.add(v.asText());
        }

        if (columns.isEmpty()) return;

        String sql = "INSERT INTO " + table + " (" + String.join(", ", columns) + ") VALUES (" + String.join(", ", placeholders) + ")";
        try {
            jdbcTemplate.update(sql, values.toArray());
        } catch (org.springframework.dao.DuplicateKeyException e) {
            // Duplicate key error: just log and continue
            System.out.println("Duplicate key for table " + table + ": " + e.getMessage());
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            // Teradata duplicate error or SQLState 23000: just log and continue
            if (e.getMessage() != null && (e.getMessage().contains("[Error 2801]") || e.getMessage().contains("SQLState 23000") || e.getMessage().toLowerCase().contains("duplicate"))) {
                System.out.println("Duplicate key for table " + table + ": " + e.getMessage());
            } else {
                throw e;
            }
        }
    }



}
