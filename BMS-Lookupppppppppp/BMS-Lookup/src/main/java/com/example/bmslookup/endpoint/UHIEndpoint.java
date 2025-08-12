package com.example.bmslookup.endpoint;
import com.example.bmslookup.service.NominationService;
import com.example.bmslookup.audit.AuditLogger;
import com.example.bmslookup.dto.GenericRequest;
import com.example.bmslookup.dto.GenericResponse;
import com.example.bmslookup.dto.GGHeader;
import com.example.bmslookup.service.LookupService;
import com.example.bmslookup.util.TableValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.ArrayList;

@Endpoint
public class UHIEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(UHIEndpoint.class);

    private static final String NAMESPACE_URI = "http://teradata.com/uhi";
    private static final String LOCAL_PART = "GenericRequest";

    @Autowired
    private LookupService lookupService;

    @Autowired
    private TableValidator tableValidator;

    @Autowired
    private AuditLogger auditLogger;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private com.example.bmslookup.service.insert_Beneficiary insertBeneficiaryService;
    @Autowired
    private com.example.bmslookup.service.update_beneficiary updateBeneficiaryService;
    
    @Autowired
    private NominationService nominationService;

    @Autowired
    private com.example.bmslookup.service.split splitService;

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = LOCAL_PART)
    @ResponsePayload
    public GenericResponse handleRequest(@RequestPayload GenericRequest request) {
        long startTime = System.currentTimeMillis();
        String correlationId = request.getGGheader().getCorrelationId();
        
        logger.info("Received new request - CorrelationId: {}", correlationId);

        try {
            GGHeader.ValidationResult headerValidation = request.getGGheader().validate();
            if (!headerValidation.isValid()) {
                logger.warn("GGHeader validation failed - code: {}, message: {}",
                        headerValidation.getCode(), headerValidation.getMessage());
                GenericResponse errorResponse = createErrorResponse(headerValidation.getCode(), headerValidation.getMessage());
                
                long processingTime = System.currentTimeMillis() - startTime;
                auditLogger.logAuditValidationError(request, headerValidation.getMessage(), processingTime, getClientIpAddress());
                
                return errorResponse;
            }

            // Route to dynamic insert/update
            String indicator = request.getIndicator();
            String jsonPayload = request.getJsonPayload();
            if ("INSERT_BENEFICIARY".equalsIgnoreCase(indicator)) {
                try {
                    insertBeneficiaryService.insertBeneficiaryData(jsonPayload);
                    return createSuccessResponse("Beneficiary data inserted successfully");
                } catch (Exception e) {
                    logger.error("Insert error: ", e);
                    return createErrorResponse(500, "Insert failed: " + e.getMessage());
                }
            } else if ("UPDATE_BENEFICIARY".equalsIgnoreCase(indicator)) {
                try {
                    updateBeneficiaryService.updateBeneficiaryData(jsonPayload);
                    return createSuccessResponse("Beneficiary data updated successfully");
                } catch (Exception e) {
                    logger.error("Update error: ", e);
                    return createErrorResponse(500, "Update failed: " + e.getMessage());
                }
            } else if ("SPLIT_BENEFICIARY".equalsIgnoreCase(indicator) || "split".equalsIgnoreCase(indicator)) {
                try {
                    splitService.splitBeneficiaryData(jsonPayload);
                    return createSuccessResponse("Beneficiary split completed successfully");
                } catch (Exception e) {
                    logger.error("Split error: ", e);
                    return createErrorResponse(500, "Split failed: " + e.getMessage());
                }
            }

            // For insert, HOF, and nomination, tableName is no longer validated here. Table name is extracted from jsonPayload in respective handlers.
            if (
                !"i".equalsIgnoreCase(request.getIndicator())
                && !"hof".equalsIgnoreCase(request.getIndicator())
                && !"nomination".equalsIgnoreCase(request.getIndicator())
                && !"INSERT_BENEFICIARY".equalsIgnoreCase(request.getIndicator())
                && !"UPDATE_BENEFICIARY".equalsIgnoreCase(request.getIndicator())
                && !tableValidator.isValidTable(extractTableNameFromPayload(request))
            ) {
                logger.warn("Invalid table name: {}", extractTableNameFromPayload(request));
                GenericResponse errorResponse = createErrorResponse(400, "Invalid table name: " + extractTableNameFromPayload(request));
                
                long processingTime = System.currentTimeMillis() - startTime;
                auditLogger.logAuditValidationError(request, "Invalid table name: " + extractTableNameFromPayload(request), processingTime, getClientIpAddress());
                
                return errorResponse;
            }

            if (isEmpty(request.getIndicator())) {
                logger.warn("Missing or empty indicator");
                GenericResponse errorResponse = createErrorResponse(400, "Missing or empty indicator");
                
                long processingTime = System.currentTimeMillis() - startTime;
                auditLogger.logAuditValidationError(request, "Missing or empty indicator", processingTime, getClientIpAddress());
                
                return errorResponse;
            }

            GenericResponse response;
            switch (request.getIndicator().toLowerCase()) {
                case "i":
                    response = handleInsert(request);
                    break;
                case "u":
                    response = handleUpdate(request);
                    break;
                case "l":
                    response = handleLookup(request);
                    break;
                case "hof":
                    response = handleHOF(request);
                    break;
                case "nomination":
                    response = handleNomination(request);
                    break;
                default:
                    logger.warn("Invalid indicator: {}", request.getIndicator());
                    response = createErrorResponse(400, "Invalid indicator: " + request.getIndicator());
            }

            long processingTime = System.currentTimeMillis() - startTime;
            auditLogger.logAudit(request, response, processingTime, getClientIpAddress());
            
            return response;

        } catch (Exception e) {
            logger.error("Error processing request", e);
            GenericResponse errorResponse = createErrorResponse(500, "Internal server error: " + e.getMessage());
            
            long processingTime = System.currentTimeMillis() - startTime;
            auditLogger.logAuditError(request, e.getMessage(), processingTime, getClientIpAddress());
            
            return errorResponse;
        }
    }

    private GenericResponse handleInsert(GenericRequest request) {
        logger.info("Processing bulk Insert request (tableName now extracted from jsonPayload)");
        try {
            if (isEmpty(request.getJsonPayload())) {
                return createErrorResponse(400, "Missing jsonPayload for insert operation");
            }
            // Parse jsonPayload to extract listName and list
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(request.getJsonPayload());
            String listName = root.has("listName") ? root.get("listName").asText() : null;
            com.fasterxml.jackson.databind.JsonNode listNode = root.get("list");
            if (listName == null || listName.trim().isEmpty()) {
                return createErrorResponse(400, "Missing listName in jsonPayload");
            }
            if (listNode == null || !listNode.isArray() || listNode.size() == 0) {
                return createErrorResponse(400, "Missing or empty list array in jsonPayload");
            }
            // Validate listName
            if (!tableValidator.isValidTable(listName)) {
                logger.warn("Invalid table name (listName): {}", listName);
                return createErrorResponse(400, "Invalid table name: " + listName);
            }
            // Call bulk insert
            boolean success = lookupService.bulkInsertRecords(
                    listName,
                    listNode,
                    request.getGGheader().getOriginatingUserIdentifier()
            );
            return success ? createSuccessResponse("Bulk records inserted successfully")
                    : createErrorResponse(500, "Failed to insert records");
        } catch (LookupService.DuplicateIdException e) {
            logger.warn("Duplicate ID error: {}", e.getMessage());
            return createErrorResponse(409, "id is duplicate");
        } catch (LookupService.MissingAttributesException e) {
            logger.warn("Missing attributes error: {}", e.getMessage());
            return createErrorResponse(410, "there are attribute is missing");
        } catch (Exception e) {
            logger.error("Bulk insert operation error", e);
            return createErrorResponse(500, "Bulk insert operation failed: " + e.getMessage());
        }
    }

    private GenericResponse handleUpdate(GenericRequest request) {
        logger.info("Processing Update request for table: {}", extractTableNameFromPayload(request));
        try {
            if (isEmpty(request.getJsonPayload())) {
                return createErrorResponse(400, "Missing jsonPayload for update operation");
            }
            String tableName = extractTableNameFromPayload(request);
            String updatedBy = request.getGGheader().getOriginatingUserIdentifier();
            com.fasterxml.jackson.databind.JsonNode rootNode = new com.fasterxml.jackson.databind.ObjectMapper().readTree(request.getJsonPayload());
            if (rootNode.has("listName") && rootNode.has("list") && rootNode.get("list").isArray()) {
                // Bulk update
                com.fasterxml.jackson.databind.JsonNode listNode = rootNode.get("list");
                int total = listNode.size();
                int successCount = 0;
                for (com.fasterxml.jackson.databind.JsonNode item : listNode) {
                    if (!item.has("id") || item.get("id").asText().trim().isEmpty()) {
                        logger.warn("Missing id in one of the bulk update items");
                        continue;
                    }
                    boolean success = lookupService.updateRecord(
                        tableName,
                        item.get("id").asText(),
                        item.toString(),
                        updatedBy
                    );
                    if (success) successCount++;
                }
                if (successCount == total) {
                    return createSuccessResponse("Bulk update completed successfully");
                } else if (successCount == 0) {
                    return createErrorResponse(404, "No records updated. Check IDs and data.");
                } else {
                    return createErrorResponse(207, "Partial success: " + successCount + "/" + total + " records updated.");
                }
            } else {
                // Fallback: single update for legacy requests
                if (isEmpty(request.getId())) {
                    return createErrorResponse(400, "Missing ID for single update operation");
                }
                boolean success = lookupService.updateRecord(
                        tableName,
                        request.getId(),
                        request.getJsonPayload(),
                        updatedBy
                );
                return success ? createSuccessResponse("Record updated successfully")
                        : createErrorResponse(404, "Record not found or update failed");
            }
        } catch (Exception e) {
            logger.error("Update operation error", e);
            return createErrorResponse(500, "Update operation failed: " + e.getMessage());
        }
    }


    private GenericResponse handleLookup(GenericRequest request) {
        // Helper to extract table name from jsonPayload (listName) or fallback to '-'.
        // Use this for all lookup operations.

        logger.info("Processing Lookup request for table: {}", extractTableNameFromPayload(request));
        try {
            String result;
            if (!isEmpty(request.getId())) {
                result = lookupService.lookupById(extractTableNameFromPayload(request), request.getId());
            } else if (!isEmpty(request.getJsonPayload())) {
                result = lookupService.lookupByCriteria(extractTableNameFromPayload(request), request.getJsonPayload());
            } else {
                result = lookupService.lookupAll(extractTableNameFromPayload(request));
            }
            return result != null ? createSuccessResponseWithData("Lookup completed successfully", result)
                    : createErrorResponse(404, "No records found");
        } catch (Exception e) {
            logger.error("Lookup operation error", e);
            return createErrorResponse(500, "Lookup operation failed: " + e.getMessage());
        }
    }

    private GenericResponse createSuccessResponse(String message) {
        GenericResponse response = new GenericResponse();
        response.setResponseCode(200);
        response.setResponseMessage(message);
        response.setTimestamp(getCurrentTimestamp());
        return response;
    }

    private GenericResponse createSuccessResponseWithData(String message, String data) {
        GenericResponse response = createSuccessResponse(message);
        response.setData(data);
        return response;
    }

    private GenericResponse createErrorResponse(int code, String message) {
        GenericResponse response = new GenericResponse();
        response.setResponseCode(code);
        response.setResponseMessage(message);
        response.setTimestamp(getCurrentTimestamp());
        return response;
    }

    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    private XMLGregorianCalendar getCurrentTimestamp() {
        try {
            LocalDateTime now = LocalDateTime.now();
            GregorianCalendar calendar = GregorianCalendar.from(now.atZone(ZoneId.systemDefault()));
            return DatatypeFactory.newInstance().newXMLGregorianCalendar(calendar);
        } catch (Exception e) {
            logger.error("Error creating timestamp", e);
            return null;
        }
    }

    /**
     * Get real client IP address
     */
    private String getClientIpAddress() {
        try {
            // Try to get from HTTP request first
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attributes != null) {
                HttpServletRequest request = attributes.getRequest();
                
                // Check for X-Forwarded-For header (for proxy/load balancer)
                String xForwardedFor = request.getHeader("X-Forwarded-For");
                if (xForwardedFor != null && !xForwardedFor.isEmpty() && !"unknown".equalsIgnoreCase(xForwardedFor)) {
                    return xForwardedFor.split(",")[0].trim();
                }
                
                // Check for X-Real-IP header
                String xRealIp = request.getHeader("X-Real-IP");
                if (xRealIp != null && !xRealIp.isEmpty() && !"unknown".equalsIgnoreCase(xRealIp)) {
                    return xRealIp;
                }
                
                // Get remote address
                String remoteAddr = request.getRemoteAddr();
                if (remoteAddr != null && !remoteAddr.isEmpty()) {
                    return remoteAddr;
                }
            }
            
            // Fallback: get local machine IP
            InetAddress localHost = InetAddress.getLocalHost();
            return localHost.getHostAddress();
            
        } catch (Exception e) {
            logger.warn("Could not get client IP address", e);
            return "unknown";
        }
    }

    /**
     * Helper to extract table name from jsonPayload (listName) or fallback to '-'.
     */
    private String extractTableNameFromPayload(GenericRequest request) {
        String jsonPayload = request.getJsonPayload();
        if (jsonPayload != null && !jsonPayload.trim().isEmpty()) {
            try {
                com.fasterxml.jackson.databind.JsonNode node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(jsonPayload);
                if (node.has("listName") && node.get("listName").isTextual()) {
                    return node.get("listName").asText();
                }
            } catch (Exception e) {
                // ignore, fallback below
            }
        }
        return "-";
    }

    /**
     * Validate HOF beneficiary data with support for nullable fields
     */
    private boolean performInsert(String tableName, com.fasterxml.jackson.databind.node.ObjectNode data) {
    try {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        List<Object> parameters = new ArrayList<>();
        
        data.fields().forEachRemaining(entry -> {
            String fieldName = entry.getKey();
            com.fasterxml.jackson.databind.JsonNode fieldValue = entry.getValue();
            
            if (columns.length() > 0) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append("\"").append(fieldName).append("\"");
            values.append("?");
            
            if (fieldValue.isNull()) {
                parameters.add(null);
            } else if (fieldValue.isTextual()) {
                String textValue = fieldValue.asText();
                parameters.add(textValue);
            } else if (fieldValue.isNumber()) {
                parameters.add(fieldValue.asText());
            } else if (fieldValue.isBoolean()) {
                parameters.add(fieldValue.asBoolean());
            } else {
                parameters.add(fieldValue.asText());
            }
        });
        
        String insertQuery = "INSERT INTO " + tableName + " (" + columns.toString() + ") VALUES (" + values.toString() + ")";
        logger.info("HOF Insert Query: {}", insertQuery);
        logger.info("HOF Insert Parameters: {}", parameters);
        
        int rowsAffected = jdbcTemplate.update(insertQuery, parameters.toArray());
        logger.info("HOF insert successful for table {}, rows affected: {}", tableName, rowsAffected);
        return rowsAffected > 0;
        
    } catch (Exception e) {
        logger.error("HOF insert failed for table {}: {}", tableName, e.getMessage());
        return false;
    }
}

private boolean updateHOFRecord(String tableName, com.fasterxml.jackson.databind.node.ObjectNode data, String idValue) {
    try {
        StringBuilder setClause = new StringBuilder();
        List<Object> parameters = new ArrayList<>();
        String idFieldName = "id";
        
        data.fields().forEachRemaining(entry -> {
            String fieldName = entry.getKey();
            com.fasterxml.jackson.databind.JsonNode fieldValue = entry.getValue();
            
            if (!fieldName.equals(idFieldName)) {
                if (setClause.length() > 0) {
                    setClause.append(", ");
                }
                setClause.append("\"").append(fieldName).append("\" = ?");
                
                if (fieldValue.isNull()) {
                    parameters.add(null);
                } else if (fieldValue.isTextual()) {
                    parameters.add(fieldValue.asText());
                } else if (fieldValue.isNumber()) {
                    parameters.add(fieldValue.asText());
                } else if (fieldValue.isBoolean()) {
                    parameters.add(fieldValue.asBoolean());
                } else {
                    parameters.add(fieldValue.asText());
                }
            }
        });
        
        parameters.add(idValue);
        
        String updateQuery = "UPDATE " + tableName + " SET " + setClause.toString() + " WHERE " + idFieldName + " = ?";
        logger.info("HOF Update Query: {}", updateQuery);
        logger.info("HOF Update Parameters: {}", parameters);
        
        int rowsAffected = jdbcTemplate.update(updateQuery, parameters.toArray());
        logger.info("HOF update successful for table {}, rows affected: {}", tableName, rowsAffected);
        return rowsAffected > 0;
        
    } catch (Exception e) {
        logger.error("HOF update failed for table {}: {}", tableName, e.getMessage());
        return false;
    }
}

private String validateHOFBeneficiaryData(com.fasterxml.jackson.databind.node.ObjectNode beneficiaryData) {
        // Define required fields that cannot be null or empty
        String[] requiredFields = {
            "id", "firstName", "lastName", "fullName", "dob", "gender", 
            "mobile", "nationality", "nationalId", "email"
        };
        
        // Define fields that can be null (optional fields)
        String[] nullableFields = {
            "previousBeneficiaryId", "deactivationReason", "medicalConditions", 
            "dateOfDeath", "husbandNationalId", "uhiaDeathReportingDate",
            "passportNumber", "passportExpiryDate", "socialInsuranceNumber"
        };
        
        // Check required fields
        for (String field : requiredFields) {
            if (!beneficiaryData.has(field) || 
                beneficiaryData.get(field).isNull() || 
                (beneficiaryData.get(field).isTextual() && beneficiaryData.get(field).asText().trim().isEmpty())) {
                return "Missing required attribute: attribute is missed: " + field;
            }
        }
        
        // For nullable fields, we don't validate - they can be null or missing
        return null; // No validation errors
    }

    /**
     * Insert HOF record directly without LookupService validation
     */
    private boolean insertHOFRecord(String tableName, com.fasterxml.jackson.databind.node.ObjectNode data, String createdBy) {
    try {
        // Upsert flow: if record with same id exists -> update, else -> insert
        String idFieldName = "id";
        if (!data.has(idFieldName) || data.get(idFieldName).isNull() || data.get(idFieldName).asText().trim().isEmpty()) {
            logger.warn("insertHOFRecord called without a valid 'id' field for table {}. Will attempt plain insert.", tableName);
            return performInsert(tableName, data);
        }

        String idValue = data.get(idFieldName).asText();
        logger.info("Upsert check for table {} with id: {}", tableName, idValue);

        // Existence check
        String existsQuery = "SELECT COUNT(1) FROM " + tableName + " WHERE " + idFieldName + " = ?";
        Integer count = jdbcTemplate.queryForObject(existsQuery, new Object[]{idValue}, Integer.class);
        boolean exists = (count != null && count > 0);
        logger.info("Record existence for table {} id {}: {}", tableName, idValue, exists);

        if (exists) {
            // Update existing
            logger.info("Record exists. Performing UPDATE for table {} id {}", tableName, idValue);
            return updateHOFRecord(tableName, data, idValue);
        } else {
            // Insert new
            logger.info("Record does not exist. Performing INSERT for table {} id {}", tableName, idValue);
            return performInsert(tableName, data);
        }
    } catch (Exception e) {
        logger.error("HOF upsert failed for table {}: {}", tableName, e.getMessage(), e);
        return false;
    }
}
    /**
     * Validate HOF employment data with support for nullable fields
     */
    private String validateHOFEmploymentData(com.fasterxml.jackson.databind.node.ObjectNode employmentData) {
        // List of required non-nullable fields for employment
        String[] requiredFields = {
            "id", "beneficiaryId", "netIncome", "jobDescription", "job", 
            "employerGovernerate", "companySocialInsuranceId"
        };
        
        // Check required fields
        for (String field : requiredFields) {
            if (!employmentData.has(field) || 
                employmentData.get(field).isNull() || 
                (employmentData.get(field).isTextual() && employmentData.get(field).asText().trim().isEmpty())) {
                return "Missing required attribute: attribute is missed: " + field;
            }
        }
        
        // For nullable fields, we don't validate - they can be null or missing
        return null; // No validation errors
    }
    /**
     * Handle HOF (Head of Family) enrollment request
     */
    private GenericResponse handleHOF(GenericRequest request) {
        logger.info("Processing HOF enrollment request");
        try {
            if (isEmpty(request.getJsonPayload())) {
                return createErrorResponse(400, "Missing jsonPayload for HOF operation");
            }

            // Parse the HOF enrollment data
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(request.getJsonPayload());
            
            if (!root.has("hofEnrollmentData")) {
                return createErrorResponse(400, "Missing hofEnrollmentData in jsonPayload");
            }
            
            com.fasterxml.jackson.databind.JsonNode hofEnrollmentData = root.get("hofEnrollmentData");
            
            if (!hofEnrollmentData.has("hofData") || !hofEnrollmentData.get("hofData").isArray()) {
                return createErrorResponse(400, "Missing or invalid hofData array");
            }
            
            com.fasterxml.jackson.databind.JsonNode hofDataArray = hofEnrollmentData.get("hofData");
            
            if (hofDataArray.size() == 0) {
                return createErrorResponse(400, "Empty hofData array");
            }
            
            // Process each HOF member
            for (com.fasterxml.jackson.databind.JsonNode hofMember : hofDataArray) {
                if (!hofMember.has("id") || hofMember.get("id").asText().trim().isEmpty()) {
                    return createErrorResponse(400, "Missing id in hofData member");
                }
                
                String beneficiaryId = hofMember.get("id").asText();
                logger.info("Processing HOF member with id: {}", beneficiaryId);
                
                // Extract beneficiary data (excluding contacts and employments arrays)
                com.fasterxml.jackson.databind.node.ObjectNode beneficiaryData = mapper.createObjectNode();
                hofMember.fields().forEachRemaining(entry -> {
                    String fieldName = entry.getKey();
                    if (!"contacts".equals(fieldName) && !"employments".equals(fieldName)) {
                        beneficiaryData.set(fieldName, entry.getValue());
                    }
                });
                
                // Validate HOF beneficiary data with nullable field support
                String validationError = validateHOFBeneficiaryData(beneficiaryData);
                if (validationError != null) {
                    return createErrorResponse(410, "Missing required attributes in HOF enrollment: " + validationError);
                }
                
                // Insert beneficiary record using direct HOF insert (bypass LookupService validation)
                boolean beneficiaryInserted = insertHOFRecord(
                    "GDEV1T_UHI_DATA.beneficiary",
                    beneficiaryData,
                    request.getGGheader().getOriginatingUserIdentifier()
                );
                
                if (!beneficiaryInserted) {
                    return createErrorResponse(500, "Failed to insert beneficiary with id: " + beneficiaryId);
                }
                
                logger.info("Successfully inserted beneficiary: {}", beneficiaryId);
                
                // Process contacts if present
                if (hofMember.has("contacts") && hofMember.get("contacts").isArray()) {
                    logger.info("Found 'contacts' array for beneficiary: {}. Processing...", beneficiaryId);
                    com.fasterxml.jackson.databind.JsonNode contactsArray = hofMember.get("contacts");
                    for (com.fasterxml.jackson.databind.JsonNode contact : contactsArray) {
                        com.fasterxml.jackson.databind.node.ObjectNode contactData = mapper.createObjectNode();
                        
                        // Set beneficiaryId
                        contactData.put("beneficiaryId", beneficiaryId);
                        
                        // Copy contact fields, ensuring beneficiaryId is not duplicated
                        contact.fields().forEachRemaining(entry -> {
                            // Ignore beneficiaryID from payload to prevent duplicate column reference
                            if (!"beneficiaryID".equalsIgnoreCase(entry.getKey())) {
                                contactData.set(entry.getKey(), entry.getValue());
                            }
                        });
                        
                        // Use insertHOFRecord for consistent upsert logic
                        boolean contactUpserted = insertHOFRecord(
                            "GDEV1T_UHI_DATA.contact",
                            contactData,
                            request.getGGheader().getOriginatingUserIdentifier()
                        );
                        
                        if (contactUpserted) {
                            logger.info("Successfully upserted contact for beneficiary: {}", beneficiaryId);
                        } else {
                            logger.warn("Failed to upsert contact for beneficiary: {}", beneficiaryId);
                        }
                    }
                } else {
                    logger.info("No 'contacts' array found for beneficiary: {}. Skipping.", beneficiaryId);
                }
                
                // Process employments if present
                if (hofMember.has("employments") && hofMember.get("employments").isArray()) {
                    logger.info("Found 'employments' array for beneficiary: {}. Processing...", beneficiaryId);
                    com.fasterxml.jackson.databind.JsonNode employmentsArray = hofMember.get("employments");
                    for (com.fasterxml.jackson.databind.JsonNode employment : employmentsArray) {
                        com.fasterxml.jackson.databind.node.ObjectNode employmentData = mapper.createObjectNode();
                        
                        // Set beneficiaryId if not present
                        if (!employment.has("beneficiaryId")) {
                            employmentData.put("beneficiaryId", beneficiaryId);
                        }
                        
                        // Copy employment fields
                        employment.fields().forEachRemaining(entry -> {
                            employmentData.set(entry.getKey(), entry.getValue());
                        });
                        
                        // Debug: Log employment data before validation
                        logger.info("Employment data before validation: {}", employmentData.toString());
                        
                        // Validate HOF employment data with nullable field support
                        String employmentValidationError = validateHOFEmploymentData(employmentData);
                        if (employmentValidationError != null) {
                            logger.warn("HOF employment validation failed: {}", employmentValidationError);
                            return createErrorResponse(410, "Missing required attributes in HOF enrollment: " + employmentValidationError);
                        }
                        
                        logger.info("HOF employment validation passed, calling HOF insert...");
                        boolean employmentInserted = insertHOFRecord(
                            "GDEV1T_UHI_DATA.employment",
                            employmentData,
                            request.getGGheader().getOriginatingUserIdentifier()
                        );
                        
                        if (employmentInserted) {
                            logger.info("Successfully inserted employment for beneficiary: {}", beneficiaryId);
                        } else {
                            logger.warn("Failed to insert employment for beneficiary: {}", beneficiaryId);
                        }
                    }
                }
            }
            
            return createSuccessResponse("HOF enrollment completed successfully");
            
        } catch (LookupService.DuplicateIdException e) {
            logger.warn("Duplicate ID error in HOF enrollment: {}", e.getMessage());
            return createErrorResponse(409, "Duplicate ID in HOF enrollment: " + e.getMessage());
        } catch (LookupService.MissingAttributesException e) {
            logger.warn("Missing attributes error in HOF enrollment: {}", e.getMessage());
            return createErrorResponse(410, "Missing required attributes in HOF enrollment: " + e.getMessage());
        } catch (Exception e) {
            logger.error("HOF enrollment operation error", e);
            return createErrorResponse(500, "HOF enrollment operation failed: " + e.getMessage());
        }
    }
    
    /**
     * Handle Nomination request
     * Deactivates the old Head of Family and updates dependents with new familyId
     */
    private GenericResponse handleNomination(GenericRequest request) {
        logger.info("Processing Nomination request by delegating to transactional service");
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(request.getJsonPayload());

            // Extract IDs from payload
            String oldFamilyId = root.at("/nominationData/nominationInfo/old_familyId").asText(null);
            String newFamilyId = root.at("/nominationData/nominationInfo/new_familyId").asText(null);

            if (oldFamilyId == null || newFamilyId == null) {
                return createErrorResponse(400, "Missing old_familyId or new_familyId in payload");
            }

            // Extract the new Head of Family's ID from the payload
            com.fasterxml.jackson.databind.JsonNode newHofJson = root.at("/nominationData/hofData/0");
            if (newHofJson.isMissingNode()) {
                return createErrorResponse(400, "Missing hofData in payload");
            }
            String newHofId = newHofJson.at("/id").asText(null);
            if (newHofId == null) {
                return createErrorResponse(400, "Missing id in hofData");
            }

            // Delegate the entire operation to the transactional service method
            String createdBy = request.getGGheader().getOriginatingUserIdentifier();
            nominationService.processNomination(oldFamilyId, newFamilyId, newHofId, createdBy);

            return createSuccessResponse("Nomination completed successfully");

        } catch (Exception e) {
            logger.error("Nomination operation failed", e);
            // The service layer will throw an exception on failure, which we catch here.
            return createErrorResponse(500, "Nomination operation failed: " + e.getMessage());
        }
    }
    


    
}