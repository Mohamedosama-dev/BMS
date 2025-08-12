
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
 * InsertHOFEnrollment: Main entry point for processing HOF enrollment data.
 * UpsertRecord: Handles inserting or updating a record based on whether it exists.
 * RecordExists: Checks if a record exists in the database.
 * UpdateRecord: Updates an existing record.
 * InsertRecord: Inserts a new record.
 */
@Service
@Transactional // Ensures all database operations in this class are wrapped in a transaction. If an exception occurs, changes are rolled back.
public class EnrollHOFService {
    private static final Logger logger = LoggerFactory.getLogger(EnrollHOFService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;          // JDBC template for database operations(Simplifies database operations (queries, inserts, updates))
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Insert HOF enrollment data into the database.
     * @param hofEnrollmentDataNode the root node of hofEnrollmentData
     * @return true if all records inserted, false otherwise
     * Purpose: Processes a JSON object (hofEnrollmentDataNode) containing an array of HOF data (hofData) and inserts/updates records in the hof, hof_employment, and hof_contact tables.
        Input: A JsonNode representing the JSON structure, expected to have a hofData array.
        Logic:
            1- Validation: Checks if hofData exists and is an array. If not, logs an error and returns false.
            2- Iteration: Loops through each hofNode in the hofData array:
                Calls upsertRecord to insert/update the HOF record in the hof table.
                If hofNode contains an employments array, processes each employment record and upserts it into the hof_employment table.
                If hofNode contains a contacts array, processes each contact record and upserts it into the hof_contact table.
            3- Error Handling: Wraps the logic in a try-catch block. If an exception occurs, logs the error and returns false.
            4-Return: Returns true if all operations succeed, false otherwise.
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
     * Purpose: Determines whether to insert or update a record based on its existence in the specified table.
        Logic:
            1- Checks if the recordNode has an id field. If not, logs a warning and skips processing.
            2- Extracts the id as a string.
            3- Calls recordExists to check if a record with the given id exists in the table:
                *   If it exists, calls updateRecord to update the record.
                *   If it doesn’t exist, calls insertRecord to insert a new record.
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
     * Purpose: Checks if a record with the given id exists in the specified table.
     * Logic:
        1- Constructs a SQL query: SELECT COUNT(*) FROM tableName WHERE id = ?.
        2- Uses JdbcTemplate.queryForObject to execute the query and retrieve the count.
        3- Returns true if the count is greater than 0, false otherwise.
        4- Catches and logs any exceptions, returning false to avoid breaking the upsert process.
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
     * Initialize SQL:
        1- Creates a StringBuilder to build the SQL query, starting with UPDATE tableName SET.
        2- Example: If tableName is hof, the query begins as UPDATE hof SET.
     * Iterate Over JSON Fields:
        1- Uses recordNode.fieldNames() to get all field names in the JSON object (e.g., id, name, age).
        2- Iterates through each field using an Iterator.
     * Skip id Field:
        If the field is id, it’s skipped (if ("id".equals(field)) continue;)
        because the id is used in the WHERE clause, not the SET clause.
     * Build SET Clause:
        1- Adds a comma (, ) between fields, but not before the first field (controlled by the first boolean flag).
        2- For each field (except id):
            * Appends the field name and = to the SQL (e.g., name = ).
            * Checks the value’s type using JsonNode methods:
                Textual values (value.isTextual()): Wraps the value in single quotes
                and escapes any existing single quotes (e.g., O'Brien becomes 'O''Brien').
            * Boolean values (value.isBoolean()): Converts true to 1 and false to 0 (common for databases that store booleans as integers).
            * Other types: Uses value.asText() to convert the value to a string (e.g., numbers like 30 become '30').
     * Append WHERE Clause:
        Adds  WHERE id = 'id' to the query, using the id passed as a parameter.
        Escapes any single quotes in the id to prevent SQL injection (e.g., id=O'Brien becomes 'O''Brien').
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
        jdbcTemplate.update(sql.toString()); //Executes the query
    }

    /**
     * Insert a single record into the specified table.
     * @param tableName the table name
     * @param recordNode the record as JsonNode
     */
    /*
     * Initialize Builders:
        1- Creates two StringBuilder objects:
            columns: For column names (e.g., id, name, age).
            values: For corresponding values (e.g., '1', 'John Doe', '30').
     *Iterate Over JSON Fields:
        1- Uses recordNode.fieldNames() to get all field names.
        2- For each field:
            Appends the field name to columns.
            Converts the field’s value to text using asText(), escapes single quotes (e.g., O'Brien becomes 'O''Brien'),
            and wraps it in single quotes.
            Adds commas between fields/values if more fields remain (it.hasNext()).
     *Build INSERT Query:
        Constructs the SQL using String.format: INSERT INTO tableName (columns) VALUES (values).
        Example: INSERT INTO hof (id, name, age) VALUES ('1', 'John Doe', '30').
     */
    private void insertRecord(String tableName, JsonNode recordNode) {
        // Build insert SQL dynamically based on recordNode fields
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (java.util.Iterator<String> it = recordNode.fieldNames(); it.hasNext(); ) {     // hasNext() checks if there are more fields to process returns true if there are more fields
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
