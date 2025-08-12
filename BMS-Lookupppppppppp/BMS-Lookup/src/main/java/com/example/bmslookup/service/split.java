package com.example.bmslookup.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.sql.ResultSet;

/**
 * خدمة تقسيم المستفيدين - تحديث familyId للمستفيدين المحددين
 */
@Service
@Transactional
public class split {
    
    private static final Logger logger = LoggerFactory.getLogger(split.class);
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * تنفيذ عملية تقسيم المستفيدين
     * @param jsonPayload البيانات التي تحتوي على معلومات التقسيم
     * @throws Exception في حالة حدوث خطأ
     */
    public void splitBeneficiaryData(String jsonPayload) throws Exception {
        logger.info("بدء عملية تقسيم المستفيدين");
        
        JsonNode root = objectMapper.readTree(jsonPayload);
        JsonNode splitData = root.get("splitData");
        
        if (splitData == null) {
            throw new Exception("splitData node missing");
        }
        
        JsonNode splitInfo = splitData.get("splitInfo");
        if (splitInfo == null) {
            throw new Exception("splitInfo node missing");
        }
        
        String oldFamilyId = splitInfo.get("old_familyId").asText();
        String newFamilyId = splitInfo.get("new_familyId").asText();
        String splitDate = splitInfo.get("splitDate").asText();
        
        logger.info("تحديث familyId من {} إلى {} بتاريخ {}", oldFamilyId, newFamilyId, splitDate);
        
        JsonNode beneficiaries = splitData.get("beneficiaryData");
        if (beneficiaries == null || !beneficiaries.isArray()) {
            throw new Exception("beneficiaryData array missing");
        }
        
        // معالجة كل مستفيد
        for (JsonNode beneficiary : beneficiaries) {
            String beneficiaryId = beneficiary.get("id").asText();
            logger.info("معالجة المستفيد: {}", beneficiaryId);
            
            // التحقق من وجود المستفيد
            if (!beneficiaryExists(beneficiaryId)) {
                logger.error("المستفيد غير موجود: {}", beneficiaryId);
                throw new Exception("Beneficiary not found: " + beneficiaryId);
            }
            
            // تحديث بيانات المستفيد الأساسية
            updateBeneficiaryFamilyId(beneficiaryId, newFamilyId, splitDate);
            
            // تحديث بيانات الوظائف إذا كانت موجودة
            if (beneficiary.has("employments") && beneficiary.get("employments").isArray()) {
                upsertEmployments(beneficiary.get("employments"), beneficiaryId, newFamilyId, splitDate);
            }
            
            // تحديث بيانات الاتصال إذا كانت موجودة
            if (beneficiary.has("contacts") && beneficiary.get("contacts").isArray()) {
                upsertContacts(beneficiary.get("contacts"), beneficiaryId, newFamilyId, splitDate);
            }
            
            logger.info("تم تحديث المستفيد بنجاح: {}", beneficiaryId);
        }
        
        logger.info("تمت عملية تقسيم المستفيدين بنجاح");
    }
    
    /**
     * التحقق من وجود المستفيد
     */
    private boolean beneficiaryExists(String beneficiaryId) {
        String sql = "SELECT COUNT(*) FROM GDEV1T_UHI_DATA.beneficiary WHERE id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, new Object[]{beneficiaryId}, Integer.class);
        return count != null && count > 0;
    }
    
    /**
     * تحديث familyId للمستفيد
     */
    private void updateBeneficiaryFamilyId(String beneficiaryId, String newFamilyId, String splitDate) {
        // البحث عن المستفيد أولاً للحصول على familyId القديم
        Map<String, Object> currentBeneficiary = findBeneficiaryById(beneficiaryId);
        if (currentBeneficiary == null) {
            logger.warn("المستفيد غير موجود: {}", beneficiaryId);
            return;
        }
        
        String currentFamilyId = (String) currentBeneficiary.get("familyId");
        logger.info("familyId الحالي للمستفيد {}: {}", beneficiaryId, currentFamilyId);
        
        // تحديث familyId إلى القيمة الجديدة
        String sql = "UPDATE GDEV1T_UHI_DATA.beneficiary SET familyId = ?, updatedAt = CURRENT_TIMESTAMP WHERE id = ?";
        
        int rowsUpdated = jdbcTemplate.update(sql, newFamilyId, beneficiaryId);
        
        if (rowsUpdated > 0) {
            logger.info("تم تحديث familyId للمستفيد {} من {} إلى {}", beneficiaryId, currentFamilyId, newFamilyId);
        } else {
            logger.warn("فشل في تحديث familyId للمستفيد {}", beneficiaryId);
        }
    }
    
    /**
     * تحديث أو إدراج بيانات الوظائف
     */
    private void upsertEmployments(JsonNode employments, String beneficiaryId, String newFamilyId, String splitDate) {
        Set<String> validColumns = getTableColumns("GDEV1T_UHI_DATA.employment");
        for (JsonNode employment : employments) {
            if (!employment.has("id")) continue;
            String employmentId = employment.get("id").asText();
            boolean exists = existsInTable("GDEV1T_UHI_DATA.employment", employmentId);
            if (exists) {
                // Dynamic update
                List<String> setClauses = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                Iterator<Map.Entry<String, JsonNode>> fields = employment.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String col = entry.getKey();
                    JsonNode v = entry.getValue();
                    if (v.isContainerNode()) continue;
                    if (!validColumns.contains(col)) continue;
                    if ("id".equalsIgnoreCase(col)) continue;
                    setClauses.add(quoteColumn(col) + " = ?");
                    if (v.isNull()) values.add(null);
                    else if (v.isNumber()) values.add(v.numberValue());
                    else if (v.isBoolean()) values.add(v.booleanValue());
                    else values.add(v.asText());
                }
                if (!setClauses.isEmpty()) {
                    String sql = "UPDATE GDEV1T_UHI_DATA.employment SET " + String.join(", ", setClauses) + " WHERE id = ?";
                    values.add(employmentId);
                    jdbcTemplate.update(sql, values.toArray());
                    logger.info("تم تحديث بيانات الوظيفة {} للمستفيد {}", employmentId, beneficiaryId);
                }
            } else {
                // Dynamic insert
                List<String> columns = new ArrayList<>();
                List<String> placeholders = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                Iterator<Map.Entry<String, JsonNode>> fields = employment.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String col = entry.getKey();
                    JsonNode v = entry.getValue();
                    if (v.isContainerNode()) continue;
                    if (!validColumns.contains(col)) continue;
                    columns.add(quoteColumn(col));
                    placeholders.add("?");
                    if (v.isNull()) values.add(null);
                    else if (v.isNumber()) values.add(v.numberValue());
                    else if (v.isBoolean()) values.add(v.booleanValue());
                    else values.add(v.asText());
                }
                if (!columns.isEmpty()) {
                    String sql = "INSERT INTO GDEV1T_UHI_DATA.employment (" + String.join(", ", columns) + ") VALUES (" + String.join(", ", placeholders) + ")";
                    jdbcTemplate.update(sql, values.toArray());
                    logger.info("تم إدراج بيانات الوظيفة {} للمستفيد {}", employmentId, beneficiaryId);
                }
            }
        }
    }

    private boolean existsInTable(String table, String id) {
        String sql = "SELECT COUNT(*) FROM " + table + " WHERE id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, new Object[]{id}, Integer.class);
        return count != null && count > 0;
    }

    private Set<String> getTableColumns(String tableName) {
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
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            } finally {
                conn.close();
            }
        } catch (Exception e) {
            // fallback: empty set
        }
        return columns;
    }

    private String quoteColumn(String col) {
        if (col.matches(".*[ ()].*")) {
            return "\"" + col + "\"";
        }
        return col;
    }
    
   
    private void upsertContacts(JsonNode contacts, String beneficiaryId, String newFamilyId, String splitDate) {
        Set<String> validColumns = getTableColumns("GDEV1T_UHI_DATA.contact");
        for (JsonNode contact : contacts) {
            if (!contact.has("id")) continue;
            String contactId = contact.get("id").asText();
            boolean exists = existsInTable("GDEV1T_UHI_DATA.contact", contactId);
            if (exists) {
                // Dynamic update
                List<String> setClauses = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                Iterator<Map.Entry<String, JsonNode>> fields = contact.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String col = entry.getKey();
                    JsonNode v = entry.getValue();
                    if (v.isContainerNode()) continue;
                    if (!validColumns.contains(col)) continue;
                    if ("id".equalsIgnoreCase(col)) continue;
                    setClauses.add(quoteColumn(col) + " = ?");
                    if (v.isNull()) values.add(null);
                    else if (v.isNumber()) values.add(v.numberValue());
                    else if (v.isBoolean()) values.add(v.booleanValue());
                    else values.add(v.asText());
                }
                if (!setClauses.isEmpty()) {
                    String sql = "UPDATE GDEV1T_UHI_DATA.contact SET " + String.join(", ", setClauses) + " WHERE id = ?";
                    values.add(contactId);
                    jdbcTemplate.update(sql, values.toArray());
                    logger.info("تم تحديث بيانات الاتصال {} للمستفيد {}", contactId, beneficiaryId);
                }
            } else {
                // Dynamic insert
                List<String> columns = new ArrayList<>();
                List<String> placeholders = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                Iterator<Map.Entry<String, JsonNode>> fields = contact.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String col = entry.getKey();
                    JsonNode v = entry.getValue();
                    if (v.isContainerNode()) continue;
                    if (!validColumns.contains(col)) continue;
                    columns.add(quoteColumn(col));
                    placeholders.add("?");
                    if (v.isNull()) values.add(null);
                    else if (v.isNumber()) values.add(v.numberValue());
                    else if (v.isBoolean()) values.add(v.booleanValue());
                    else values.add(v.asText());
                }
                if (!columns.isEmpty()) {
                    String sql = "INSERT INTO GDEV1T_UHI_DATA.contact (" + String.join(", ", columns) + ") VALUES (" + String.join(", ", placeholders) + ")";
                    jdbcTemplate.update(sql, values.toArray());
                    logger.info("تم إدراج بيانات الاتصال {} للمستفيد {}", contactId, beneficiaryId);
                }
            }
        }
    }
    
    /**
     * البحث عن المستفيد بواسطة ID
     */
    public Map<String, Object> findBeneficiaryById(String beneficiaryId) {
        String sql = "SELECT * FROM GDEV1T_UHI_DATA.beneficiary WHERE id = ?";
        
        try {
            return jdbcTemplate.queryForMap(sql, beneficiaryId);
        } catch (Exception e) {
            logger.error("خطأ في البحث عن المستفيد: {}", beneficiaryId, e);
            return null;
        }
    }
}