package com.example.bmslookup.audit;

import com.example.bmslookup.dto.GenericRequest;
import com.example.bmslookup.dto.GenericResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Dedicated audit logger for tracking all requests and responses
 */
@Component
public class AuditLogger {

    private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String AUDIT_LOG_FILE = "logs/audit.log";


    /**
     * Log audit information for request and response
     */
    public void logAudit(GenericRequest request, GenericResponse response, long processingTime, String ipAddress) {
        try {
            String timestamp = LocalDateTime.now().format(formatter);
            String correlationId = request.getGGheader().getCorrelationId();
            String operationType = request.getOperationType();
            String tableName = extractTableNameFromPayload(request);
            String userId = request.getGGheader().getOriginatingUserIdentifier();
            int responseCode = response.getResponseCode();
            String responseMessage = response.getResponseMessage();
            String jsonPayload = truncateJsonPayload(request.getJsonPayload());
            String responseData = truncateJsonPayload(response.getData());

            String auditMessage = String.format(
                "AUDIT: %s | %s | %s | %s | %s | %s | %dms | %d | %s | %s | %s",
                timestamp,
                correlationId,
                operationType,
                tableName,
                userId,
                ipAddress,
                processingTime,
                responseCode,
                responseMessage,
                jsonPayload,
                responseData
            );

            writeToAuditFile(auditMessage);
            logger.debug("Audit logged: {}", correlationId);

        } catch (Exception e) {
            logger.error("Error logging audit information", e);
        }
    }

 
    public void logAuditError(GenericRequest request, String errorMessage, long processingTime, String ipAddress) {
        try {
            String timestamp = LocalDateTime.now().format(formatter);
            String correlationId = request.getGGheader().getCorrelationId();
            String operationType = request.getOperationType();
            String tableName = extractTableNameFromPayload(request);
            String userId = request.getGGheader().getOriginatingUserIdentifier();
            String jsonPayload = truncateJsonPayload(request.getJsonPayload());

            String auditMessage = String.format(
                "AUDIT_ERROR: %s | %s | %s | %s | %s | %s | %dms | ERROR | %s | %s | ",
                timestamp,
                correlationId,
                operationType,
                tableName,
                userId,
                ipAddress,
                processingTime,
                errorMessage,
                jsonPayload
            );

            writeToAuditFile(auditMessage);
            logger.debug("Audit error logged: {}", correlationId);

        } catch (Exception e) {
            logger.error("Error logging audit error information", e);
        }
    }

    /**
     * Log audit information for validation errors
     */
    public void logAuditValidationError(GenericRequest request, String validationError, long processingTime, String ipAddress) {
        try {
            String timestamp = LocalDateTime.now().format(formatter);
            String correlationId = request.getGGheader().getCorrelationId();
            String operationType = request.getOperationType();
            String tableName = extractTableNameFromPayload(request);
            String userId = request.getGGheader().getOriginatingUserIdentifier();
            String jsonPayload = truncateJsonPayload(request.getJsonPayload());

            String auditMessage = String.format(
                "AUDIT_VALIDATION: %s | %s | %s | %s | %s | %s | %dms | VALIDATION_ERROR | %s | %s | ",
                timestamp,
                correlationId,
                operationType,
                tableName,
                userId,
                ipAddress,
                processingTime,
                validationError,
                jsonPayload
            );

            writeToAuditFile(auditMessage);
            logger.debug("Audit validation error logged: {}", correlationId);

        } catch (Exception e) {
            logger.error("Error logging audit validation error information", e);
        }
    }

    /**
     * Write audit message to file
     */
    private synchronized void writeToAuditFile(String message) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(AUDIT_LOG_FILE, true))) {
            writer.println(message);
            writer.flush();
        } catch (IOException e) {
            logger.error("Error writing to audit file: {}", AUDIT_LOG_FILE, e);
        }
    }

    /**
     * Truncate JSON payload if too long
     */
    private String truncateJsonPayload(String jsonPayload) {
        if (jsonPayload == null || jsonPayload.isEmpty()) {
            return "";
        }
        
        if (jsonPayload.length() > 1000) {
            return jsonPayload.substring(0, 1000) + "... [TRUNCATED]";
        }
        
        return jsonPayload;
    }

    public String getAuditLogFilePath() {
        return AUDIT_LOG_FILE;
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
}