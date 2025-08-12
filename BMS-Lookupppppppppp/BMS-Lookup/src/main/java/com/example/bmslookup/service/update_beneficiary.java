package com.example.bmslookup.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class update_beneficiary {

    private String quoteColumn(String col) {
        if (col.matches(".*[ ()].*")) {
            return "\"" + col + "\"";
        }
        return col;
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Set<String>> tableColumnsCache = new HashMap<>();

    private Set<String> getTableColumns(String tableName) {
        if (tableColumnsCache.containsKey(tableName)) {
            return tableColumnsCache.get(tableName);
        }
        Set<String> columns = new HashSet<>();
        try (java.sql.Connection conn = jdbcTemplate.getDataSource().getConnection()) {
            java.sql.DatabaseMetaData meta = conn.getMetaData();
            String schema = null;
            String table = tableName;
            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schema = parts[0];
                table = parts[1];
            }
            try (java.sql.ResultSet rs = meta.getColumns(null, schema, table, null)) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME").toLowerCase()); // force lowercase
                }
            }
        } catch (Exception e) {
            // fallback: empty set
        }
        tableColumnsCache.put(tableName, columns);
        return columns;
    }

    public void updateBeneficiaryData(String jsonPayload) throws Exception {
        JsonNode root = objectMapper.readTree(jsonPayload);
        JsonNode updateData = root.get("updateData");
        if (updateData == null) throw new Exception("updateData node missing");
        JsonNode beneficiaries = updateData.get("beneficiaryData");
        if (beneficiaries == null || !beneficiaries.isArray()) throw new Exception("beneficiaryData array missing");

        for (JsonNode beneficiary : beneficiaries) {
            String beneficiaryId = beneficiary.get("id").asText();
            dynamicUpdate("GDEV1T_UHI_DATA.beneficiary", "id", beneficiaryId, beneficiary);

            if (beneficiary.has("employments") && beneficiary.get("employments").isArray()) {
                for (JsonNode employment : beneficiary.get("employments")) {
                    String empId = employment.get("id").asText();
                    String empBeneficiaryId = employment.has("beneficiaryId") ? employment.get("beneficiaryId").asText() : beneficiaryId;
                    dynamicUpdate("GDEV1T_UHI_DATA.employment", "id,beneficiaryId", empId + "," + empBeneficiaryId, employment);
                }
            }

            if (beneficiary.has("contacts") && beneficiary.get("contacts").isArray()) {
                for (JsonNode contact : beneficiary.get("contacts")) {
                    String contactId = contact.get("id").asText();
                    String contactBeneficiaryId = contact.has("beneficiaryId") ? contact.get("beneficiaryId").asText() : beneficiaryId;
                    dynamicUpdate("GDEV1T_UHI_DATA.contact", "id,beneficiaryId", contactId + "," + contactBeneficiaryId, contact);
                }
            }
        }
    }
    private void dynamicUpdate(String table, String keyColumns, String keyValues, JsonNode data) {
  
        Map<String, String> keyValueMap = new LinkedHashMap<>();
        String[] keyVals = keyValues.split(",");
        int i = 0;
        for (String k : keyColumns.split(",")) {
            keyValueMap.put(k.toLowerCase(), keyVals[i++]); 
        }

  
        StringBuilder checkSql = new StringBuilder("SELECT COUNT(*) FROM " + table + " WHERE ");
        List<Object> checkParams = new ArrayList<>();
        for (Map.Entry<String, String> kv : keyValueMap.entrySet()) {
            checkSql.append(kv.getKey()).append("=? AND ");
            checkParams.add(kv.getValue());
        }
        checkSql.setLength(checkSql.length() - 5); // remove last AND

        Integer count = jdbcTemplate.queryForObject(checkSql.toString(), checkParams.toArray(), Integer.class);
        if (count == null || count == 0) {
            throw new RuntimeException("BENEFICIARY ID NOT FOUND");
        }

    
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        List<String> sets = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        Set<String> keysSet = new HashSet<>(keyValueMap.keySet());
        Set<String> validColumns = getTableColumns(table);

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String col = entry.getKey().toLowerCase();
            JsonNode v = entry.getValue();

            if (keysSet.contains(col)) continue;
            if (v.isContainerNode()) continue;
            if (!validColumns.contains(col)) continue;

            sets.add(quoteColumn(col) + "=?");
            if (v.isNull()) values.add(null);
            else if (v.isNumber()) values.add(v.numberValue());
            else if (v.isBoolean()) values.add(v.booleanValue());
            else values.add(v.asText());
        }

        if (sets.isEmpty()) return;

        StringBuilder sql = new StringBuilder("UPDATE " + table + " SET " + String.join(", ", sets) + " WHERE ");
        for (Map.Entry<String, String> kv : keyValueMap.entrySet()) {
            sql.append(kv.getKey()).append("=? AND ");
            values.add(kv.getValue());
        }
        sql.setLength(sql.length() - 5);

        jdbcTemplate.update(sql.toString(), values.toArray());
    }


}
