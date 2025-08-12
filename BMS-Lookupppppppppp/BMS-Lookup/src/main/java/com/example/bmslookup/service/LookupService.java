package com.example.bmslookup.service;

import com.example.bmslookup.util.TableValidator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

/**
 * خدمة التعامل مع قاعدة البيانات Teradata
 * تدعم عمليات Insert, Update, Lookup
 */
@Service
@Transactional
public class LookupService {

    private static final Logger logger = LoggerFactory.getLogger(LookupService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TableValidator tableValidator;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * تنظيف وتصحيح JSON قبل التحليل
     * 
     * @param jsonString JSON المراد تنظيفه
     * @return JSON نظيف
     */
    private String cleanJson(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return jsonString;
        }
        
        // إزالة المسافات الزائدة والأسطر الجديدة
        String cleaned = jsonString.trim();
        
        // إزالة الفواصل الزائدة قبل الأقواس المغلقة
        cleaned = cleaned.replaceAll(",\\s*}", "}");
        cleaned = cleaned.replaceAll(",\\s*]", "]");
        
        // إزالة الفواصل الزائدة في نهاية الكائنات
        cleaned = cleaned.replaceAll(",\\s*\"\\s*}", "\"}");
        
        // إزالة الفواصل الزائدة قبل الأقواس المغلقة مع مسافات
        cleaned = cleaned.replaceAll(",\\s*\\n\\s*}", "\n}");
        cleaned = cleaned.replaceAll(",\\s*\\n\\s*]", "\n]");
        
        // إزالة الأسطر الفارغة الزائدة
        cleaned = cleaned.replaceAll("\\n\\s*\\n", "\n");
        
        // إزالة المسافات الزائدة في نهاية الأسطر
       // إزالة المسافات الزائدة في نهاية الأسطر
        cleaned = cleaned.replaceAll("(?m)\\s+$", "");
        
        logger.debug("JSON بعد التنظيف: {}", cleaned);
        return cleaned;
    }

    // استثناء مخصص لتكرار الـ id
    public static class DuplicateIdException extends RuntimeException {
        public DuplicateIdException(String message) { super(message); }
    }
    // استثناء مخصص لنقص الأعمدة
    public static class MissingAttributesException extends RuntimeException {
        public MissingAttributesException(String message) { 
            super("Missing required attribute: " + message); 
        }
    }

    /**
     * إدراج سجل جديد في الجدول المحدد
     * 
     * ملاحظة: يجب أن يحتوي JSON على جميع الأعمدة المطلوبة بما في ذلك ID
     * مثال: {"id": "1", "name": "Cairo", "country_id": "1"}
     */
    public boolean insertRecord(String tableName, String jsonPayload, String createdBy) {
        logger.info("إدراج سجل جديد في الجدول: {}", tableName);

        try {
            // فحص صحة اسم الجدول (يتضمن تحويل القيم الصغيرة تلقائياً)
            if (!tableValidator.isValidTable(tableName)) {
                logger.error("اسم جدول غير صحيح: {}", tableName);
                return false;
            }

            // الحصول على اسم الجدول المحلول
            String resolvedTableName = tableValidator.resolveTableName(tableName);
            logger.info("تم تحويل اسم الجدول من '{}' إلى '{}'", tableName, resolvedTableName);

            // تنظيف وتحليل JSON
            String cleanedJson = cleanJson(jsonPayload);
            JsonNode jsonNode = objectMapper.readTree(cleanedJson);

            // فحص تكرار id إذا كان موجود
            if (jsonNode.has("id") && !jsonNode.get("id").isNull()) {
                String idValue = jsonNode.get("id").asText();
                if (recordExists(resolvedTableName, idValue)) {
                    throw new DuplicateIdException("id is duplicate");
                }
            }

            // فحص اكتمال الأعمدة (كل الأعمدة المطلوبة يجب أن تكون موجودة وليست null)
            jdbcTemplate.query("SELECT * FROM " + resolvedTableName + " WHERE 1=0", rs -> {
                java.sql.ResultSetMetaData meta = rs.getMetaData();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String col = meta.getColumnName(i);
                    // ID مطلوب ولا يمكن تخطيه
                    if (!jsonNode.has(col) || jsonNode.get(col).isNull() || (jsonNode.get(col).isTextual() && jsonNode.get(col).asText().trim().isEmpty())) {
                        throw new MissingAttributesException("attribute is missed: " + col);
                    }
                }
                return null;
            });

            // بناء query الإدراج
            String insertQuery = buildInsertQuery(resolvedTableName, jsonNode);
            logger.debug("Insert Query: {}", insertQuery);

            // تنفيذ الإدراج
            int rowsAffected = jdbcTemplate.update(insertQuery);

            logger.info("تم إدراج {} سجل في الجدول {}", rowsAffected, resolvedTableName);
            return rowsAffected > 0;

        } catch (DuplicateIdException e) {
            throw e;
        } catch (MissingAttributesException e) {
            throw e;
        } catch (JsonProcessingException e) {
            logger.error("خطأ في تحليل JSON: {}", e.getMessage());
            logger.error("JSON المرسل: {}", jsonPayload);
            logger.error("موقع الخطأ: السطر {}, العمود {}", e.getLocation().getLineNr(), e.getLocation().getColumnNr());
            return false;
        } catch (DataAccessException e) {
            logger.error("خطأ في قاعدة البيانات أثناء الإدراج: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            logger.error("خطأ غير متوقع أثناء الإدراج: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Bulk insert multiple records into the specified table
     * @param tableName table name (from listName)
     * @param listNode array of record objects
     * @param createdBy user
     * @return true if all records inserted, false otherwise
     * @throws DuplicateIdException, MissingAttributesException if any record fails
     */
    public boolean bulkInsertRecords(String tableName, com.fasterxml.jackson.databind.JsonNode listNode, String createdBy) {
        logger.info("Bulk inserting {} records into table: {}", listNode.size(), tableName);
        if (!tableValidator.isValidTable(tableName)) {
            logger.error("اسم جدول غير صحيح: {}", tableName);
            return false;
        }
        String resolvedTableName = tableValidator.resolveTableName(tableName);
        int successCount = 0;
        for (com.fasterxml.jackson.databind.JsonNode recordNode : listNode) {
            try {
                // Check for duplicate id
                if (recordNode.has("id") && !recordNode.get("id").isNull()) {
                    String idValue = recordNode.get("id").asText();
                    if (recordExists(resolvedTableName, idValue)) {
                        throw new DuplicateIdException("id is duplicate");
                    }
                }
                // Validate required columns
                jdbcTemplate.query("SELECT * FROM " + resolvedTableName + " WHERE 1=0", rs -> {
                    java.sql.ResultSetMetaData meta = rs.getMetaData();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        String col = meta.getColumnName(i);
                        if (!recordNode.has(col) || recordNode.get(col).isNull() || (recordNode.get(col).isTextual() && recordNode.get(col).asText().trim().isEmpty())) {
                            throw new MissingAttributesException("attribute is missed: " + col);
                        }
                    }
                    return null;
                });
                // Build and execute insert query
                String insertQuery = buildInsertQuery(resolvedTableName, recordNode);
                logger.debug("Bulk Insert Query: {}", insertQuery);
                int rowsAffected = jdbcTemplate.update(insertQuery);
                logger.info("تم إدراج سجل في bulk {}: {}", resolvedTableName, rowsAffected);
                if (rowsAffected > 0) successCount++;
            } catch (DuplicateIdException | MissingAttributesException e) {
                throw e;
            } catch (Exception e) {
                logger.error("Bulk insert error for record: {}", recordNode, e);
                throw new RuntimeException("Bulk insert failed for a record: " + e.getMessage(), e);
            }
        }
        logger.info("تم إدراج {} من {} سجل بنجاح في الجدول {}", successCount, listNode.size(), resolvedTableName);
        return successCount == listNode.size();
    }

    /**
     * تحديث سجل موجود في الجدول المحدد
     */
    public boolean updateRecord(String tableName, String id, String jsonPayload, String updatedBy) {
        logger.info("تحديث السجل {} في الجدول: {}", id, tableName);

        try {
            // فحص صحة اسم الجدول (يتضمن تحويل القيم الصغيرة تلقائياً)
            if (!tableValidator.isValidTable(tableName)) {
                logger.error("اسم جدول غير صحيح: {}", tableName);
                return false;
            }

            // الحصول على اسم الجدول المحلول
            String resolvedTableName = tableValidator.resolveTableName(tableName);
            logger.info("تم تحويل اسم الجدول من '{}' إلى '{}'", tableName, resolvedTableName);

            // فحص وجود السجل
            if (!recordExists(resolvedTableName, id)) {
                logger.warn("السجل {} غير موجود في الجدول {}", id, resolvedTableName);
                return false;
            }

            // تنظيف وتحليل JSON
            String cleanedJson = cleanJson(jsonPayload);
            JsonNode jsonNode = objectMapper.readTree(cleanedJson);

            // لا يوجد فحص اكتمال الأعمدة في update

            // بناء query التحديث
            String updateQuery = buildUpdateQuery(resolvedTableName, id, jsonNode);
            logger.debug("Update Query: {}", updateQuery);

            // تنفيذ التحديث
            int rowsAffected = jdbcTemplate.update(updateQuery);

            logger.info("تم تحديث {} سجل في الجدول {}", rowsAffected, resolvedTableName);
            return rowsAffected > 0;

        } catch (JsonProcessingException e) {
            logger.error("خطأ في تحليل JSON: {}", e.getMessage());
            logger.error("JSON المرسل: {}", jsonPayload);
            logger.error("موقع الخطأ: السطر {}, العمود {}", e.getLocation().getLineNr(), e.getLocation().getColumnNr());
            return false;
        } catch (DataAccessException e) {
            logger.error("خطأ في قاعدة البيانات أثناء التحديث: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            logger.error("خطأ غير متوقع أثناء التحديث: {}", e.getMessage());
            return false;
        }
    }

    /**
     * البحث عن سجل بواسطة المعرف
     */
    public String lookupById(String tableName, String id) {
        logger.info("البحث عن السجل {} في الجدول: {}", id, tableName);

        try {
            // فحص صحة اسم الجدول (يتضمن تحويل القيم الصغيرة تلقائياً)
            if (!tableValidator.isValidTable(tableName)) {
                logger.error("اسم جدول غير صحيح: {}", tableName);
                return null;
            }

            // الحصول على اسم الجدول المحلول
            String resolvedTableName = tableValidator.resolveTableName(tableName);
            logger.info("تم تحويل اسم الجدول من '{}' إلى '{}'", tableName, resolvedTableName);

            String selectQuery = "SELECT * FROM " + resolvedTableName + " WHERE id = ?";
            
            Map<String, Object> result = jdbcTemplate.queryForObject(selectQuery, 
                new Object[]{id}, new MapRowMapper());
            
            if (result != null) {
                String jsonResult = objectMapper.writeValueAsString(result);
                logger.info("تم العثور على السجل {} في الجدول {}", id, resolvedTableName);
                return jsonResult;
            }

            return null;

        } catch (EmptyResultDataAccessException e) { 
            logger.info("لم يتم العثور على السجل {} في الجدول {}", id, tableName);
            return null;
        } catch (DataAccessException e) {
            logger.error("خطأ في قاعدة البيانات أثناء البحث: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            logger.error("خطأ غير متوقع أثناء البحث: {}", e.getMessage());
            return null;
        }
    }

    /**
     * البحث عن سجلات بواسطة معايير
     */
    public String lookupByCriteria(String tableName, String jsonCriteria) {
        logger.info("البحث في الجدول: {} بواسطة معايير", tableName);

        try {
            // فحص صحة اسم الجدول (يتضمن تحويل القيم الصغيرة تلقائياً)
            if (!tableValidator.isValidTable(tableName)) {
                logger.error("اسم جدول غير صحيح: {}", tableName);
                return null;
            }

            // الحصول على اسم الجدول المحلول
            String resolvedTableName = tableValidator.resolveTableName(tableName);
            logger.info("تم تحويل اسم الجدول من '{}' إلى '{}'", tableName, resolvedTableName);

            // تنظيف وتحليل معايير البحث
            String cleanedCriteria = cleanJson(jsonCriteria);
            JsonNode criteriaNode = objectMapper.readTree(cleanedCriteria);
            
            // بناء query البحث
            String selectQuery = buildSelectQuery(resolvedTableName, criteriaNode);
            
            List<Map<String, Object>> results = jdbcTemplate.query(selectQuery, new MapRowMapper());
            
            if (!results.isEmpty()) {
                String jsonResult = objectMapper.writeValueAsString(results);
                logger.info("تم العثور على {} سجل في الجدول {}", results.size(), resolvedTableName);
                return jsonResult;
            }

            return null;

        } catch (JsonProcessingException e) {
            logger.error("خطأ في تحليل معايير البحث: {}", e.getMessage());
            logger.error("JSON المرسل: {}", jsonCriteria);
            logger.error("موقع الخطأ: السطر {}, العمود {}", e.getLocation().getLineNr(), e.getLocation().getColumnNr());
            return null;
        } catch (DataAccessException e) {
            logger.error("خطأ في قاعدة البيانات أثناء البحث: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            logger.error("خطأ غير متوقع أثناء البحث: {}", e.getMessage());
            return null;
        }
    }

    /**
     * جلب جميع السجلات من الجدول
     */
    public String lookupAll(String tableName) {
        logger.info("جلب جميع السجلات من الجدول: {}", tableName);

        try {
            // فحص صحة اسم الجدول (يتضمن تحويل القيم الصغيرة تلقائياً)
            if (!tableValidator.isValidTable(tableName)) {
                logger.error("اسم جدول غير صحيح: {}", tableName);
                return null;
            }

            // الحصول على اسم الجدول المحلول
            String resolvedTableName = tableValidator.resolveTableName(tableName);
            logger.info("تم تحويل اسم الجدول من '{}' إلى '{}'", tableName, resolvedTableName);

            String selectQuery = "SELECT * FROM " + resolvedTableName + " ORDER BY id";
            
            List<Map<String, Object>> results = jdbcTemplate.query(selectQuery, new MapRowMapper());
            
            if (!results.isEmpty()) {
                String jsonResult = objectMapper.writeValueAsString(results);
                logger.info("تم جلب {} سجل من الجدول {}", results.size(), resolvedTableName);
                return jsonResult;
            }

            return null;

        } catch (DataAccessException e) {
            logger.error("خطأ في قاعدة البيانات أثناء جلب البيانات: {}", e.getMessage());
            return null;
        } catch (Exception e) {
            logger.error("خطأ غير متوقع أثناء جلب البيانات: {}", e.getMessage());
            return null;
        }
    }

    /**
     * فحص وجود سجل
     */
    private boolean recordExists(String tableName, String id) {
        try {
            String countQuery = "SELECT COUNT(*) FROM " + tableName + " WHERE id = ?";
            int count = jdbcTemplate.queryForObject(countQuery, Integer.class, id);
            return count > 0;
        } catch (DataAccessException e) {
            logger.error("خطأ في فحص وجود السجل: {}", e.getMessage());
            return false;
        }
    }



    /**
 * بناء query الإدراج - يتطلب ID مخصص
 */
private String buildInsertQuery(String tableName, JsonNode jsonNode) {
    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(tableName).append(" (");
    
    // إضافة الأعمدة
    Iterator<String> fieldNames = jsonNode.fieldNames();
    while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        query.append(fieldName).append(", ");
    }
    // إزالة آخر فاصلة
    query.setLength(query.length() - 2);
    query.append(") VALUES (");
    
    // إضافة القيم
    Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
    while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldName = field.getKey();
        JsonNode fieldValue = field.getValue();
        
        if (fieldValue.isTextual()) {
            query.append("'").append(fieldValue.asText().replace("'", "''")).append("', ");
        } else if (fieldValue.isBoolean()) {
            query.append(fieldValue.asBoolean() ? "1" : "0").append(", ");
        } else {
            query.append(fieldValue.asText()).append(", ");
        }
    }
    // إزالة آخر فاصلة
    query.setLength(query.length() - 2);
    query.append(")");
    
    return query.toString();
}

    private String buildUpdateQuery(String tableName, String id, JsonNode jsonNode) {
        StringBuilder query = new StringBuilder();
        query.append("UPDATE ").append(tableName).append(" SET ");
        
       
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        boolean hasFields = false;
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            
            if (!fieldName.equals("id")) { // لا نحدث id
                if (hasFields) {
                    query.append(", ");
                }
                query.append(fieldName).append(" = ");
                if (fieldValue.isTextual()) {
                    query.append("'").append(fieldValue.asText().replace("'", "''")).append("'");
                } else if (fieldValue.isBoolean()) {
                    query.append(fieldValue.asBoolean() ? "1" : "0");
                } else {
                    query.append(fieldValue.asText());
                }
                hasFields = true;
            }
        }
        
        query.append(" WHERE id = ").append(id);
        
        return query.toString();
    }

    /**
     * بناء query البحث
     */
    private String buildSelectQuery(String tableName, JsonNode criteriaNode) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ").append(tableName);
        
        if (criteriaNode != null && !criteriaNode.isEmpty()) {
            query.append(" WHERE ");
            Iterator<Map.Entry<String, JsonNode>> fields = criteriaNode.fields();
            boolean hasConditions = false;
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                
                if (hasConditions) {
                    query.append(" AND ");
                }
                query.append(fieldName).append(" = ");
                if (fieldValue.isTextual()) {
                    query.append("'").append(fieldValue.asText().replace("'", "''")).append("'");
                } else {
                    query.append(fieldValue.asText());
                }
                hasConditions = true;
            }
        }
        
        query.append(" ORDER BY id");
        return query.toString();
    }

    /**
     * RowMapper لتحويل النتائج إلى Map
     */
    private static class MapRowMapper implements RowMapper<Map<String, Object>> {
        @Override
        public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
            Map<String, Object> row = new HashMap<>();
            int columnCount = rs.getMetaData().getColumnCount();
            
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rs.getMetaData().getColumnName(i);
                Object value = rs.getObject(i);
                
                // تحويل Timestamp إلى String
                if (value instanceof Timestamp) {
                    value = value.toString();
                }
                
                row.put(columnName, value);
            }
            
            return row;
        }
    }
}