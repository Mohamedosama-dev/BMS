
package com.example.bmslookup.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Iterator;

/**
 * Service for handling Head of Family (HOF) enrollment data insertions.
 */
@Service
@Transactional
public class EnrollHOFService {
    private static final Logger logger = LoggerFactory.getLogger(EnrollHOFService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Insert HOF enrollment data into the database.
     * @param hofEnrollmentDataNode the root node of hofEnrollmentData
     * @return true if all records inserted, false otherwise
     */
    public boolean insertHOFEnrollment(JsonNode hofEnrollmentDataNode) {
        try {
            if (!hofEnrollmentDataNode.has("hofData") || !hofEnrollmentDataNode.get("hofData").isArray()) {
                logger.error("Missing or invalid hofData array");
                return false;
            }
            for (JsonNode hofNode : hofEnrollmentDataNode.get("hofData")) {
                // Upsert HOF main record
                upsertRecord("hof", hofNode);

                // Upsert employments
                if (hofNode.has("employments") && hofNode.get("employments").isArray()) {
                    for (JsonNode empNode : hofNode.get("employments")) {
                        upsertRecord("hof_employment", empNode);
                    }
                }

                // Upsert contacts
                if (hofNode.has("contacts") && hofNode.get("contacts").isArray()) {
                    for (JsonNode contactNode : hofNode.get("contacts")) {
                        upsertRecord("hof_contact", contactNode);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("Error inserting/updating HOF enrollment data", e);
            return false;
        }
    }

    /**
     * Upsert (insert or update) a record in the specified table by id.
     */
    private void upsertRecord(String tableName, JsonNode recordNode) {
        if (!recordNode.has("id")) {
            logger.warn("Skipping record for table {}: missing id field", tableName);
            return;
        }
        String id = recordNode.get("id").asText();
        if (recordExists(tableName, id)) {
            updateRecord(tableName, id, recordNode);
        } else {
            insertRecord(tableName, recordNode);
        }
    }

    /**
     * Check if a record exists in the table by id.
     */
    private boolean recordExists(String tableName, String id) {
        try {
            String sql = "SELECT COUNT(*) FROM " + tableName + " WHERE id = ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, id);
            return count != null && count > 0;
        } catch (Exception e) {
            logger.error("Error checking existence in {} for id={}: {}", tableName, id, e.getMessage());
            return false;
        }
    }

    /**
     * Update a record in the table by id.
     */
    private void updateRecord(String tableName, String id, JsonNode recordNode) {
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        Iterator<String> fields = recordNode.fieldNames();
        boolean first = true;
        while (fields.hasNext()) {
            String field = fields.next();
            if ("id".equals(field)) continue;
            if (!first) sql.append(", ");
            sql.append(field).append(" = ");
            JsonNode value = recordNode.get(field);
            if (value.isTextual()) {
                sql.append("'").append(value.asText().replace("'", "''")).append("'");
            } else if (value.isBoolean()) {
                sql.append(value.asBoolean() ? "1" : "0");
            } else {
                sql.append(value.asText());
            }
            first = false;
        }
        sql.append(" WHERE id = '").append(id.replace("'", "''")).append("'");
        logger.debug("Executing UPDATE: {}", sql);
        jdbcTemplate.update(sql.toString());
    }

    /**
     * Insert a single record into the specified table.
     * @param tableName the table name
     * @param recordNode the record as JsonNode
     */
    private void insertRecord(String tableName, JsonNode recordNode) {
        // Build insert SQL dynamically based on recordNode fields
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (java.util.Iterator<String> it = recordNode.fieldNames(); it.hasNext(); ) {
            String field = it.next();
            columns.append(field);
            values.append("'").append(recordNode.get(field).asText().replace("'", "''")).append("'");
            if (it.hasNext()) {
                columns.append(", ");
                values.append(", ");
            }
        }
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
        logger.debug("Executing SQL: {}", sql);
        jdbcTemplate.update(sql);
    }
}
