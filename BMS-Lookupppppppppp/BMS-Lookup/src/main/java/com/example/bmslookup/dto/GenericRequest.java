package com.example.bmslookup.dto;

import javax.xml.bind.annotation.*;
import java.util.Arrays;
import java.util.List;

/**
 * DTO for a generic request supporting insert/update/lookup operations.
 * Contains GGHeader and operation data.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GenericRequest", propOrder = {
    "GGheader",
    "id",
    "indicator",
    "jsonPayload"
})
@XmlRootElement(name = "GenericRequest", namespace = "http://teradata.com/uhi")
public class GenericRequest {

    // Allowed indicator values
    private static final List<String> ALLOWED_INDICATORS = Arrays.asList("i", "u", "l", "hof", "I", "U", "L", "","HOF","INSERT_BENEGICIARY", "UPDATE_BENEFICIARY","nomination","SPLIT_BENEFICIARY");

    // Maximum length limits
    private static final int MAX_TABLE_NAME_LENGTH = 100;
    private static final int MAX_ID_LENGTH = 50;
    private static final int MAX_JSON_PAYLOAD_LENGTH = 9000000;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private GGHeader GGheader;



    @XmlElement(namespace = "http://teradata.com/uhi")
    private String id;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String indicator;

    @XmlElement(namespace = "http://teradata.com/uhi")
    private String jsonPayload;

    // Constructors
    public GenericRequest() {}

    public GenericRequest(GGHeader GGheader, String id, String indicator, String jsonPayload) {
        this.GGheader = GGheader;
        this.id = id;
        this.indicator = indicator;
        this.jsonPayload = jsonPayload;
    }

    // Getters and Setters
    public GGHeader getGGheader() {
        return GGheader;
    }

    public void setGGheader(GGHeader GGheader) {
        this.GGheader = GGheader;
    }



    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIndicator() {
        return indicator;
    }

    public void setIndicator(String indicator) {
        this.indicator = indicator;
    }

    public String getJsonPayload() {
        return jsonPayload;
    }

    public void setJsonPayload(String jsonPayload) {
        this.jsonPayload = jsonPayload;
    }

    /**
     * Validates the request fields.
     *
     * @return ValidationResult indicating the outcome of validation
     */
    public ValidationResult validate() {
        // Validate GGHeader
        if (GGheader == null) {
            return new ValidationResult(301, "Missing GGHeader");
        }

        GGHeader.ValidationResult headerValidation = GGheader.validate();
        if (!headerValidation.isValid()) {
            return new ValidationResult(headerValidation.getCode(), headerValidation.getMessage());
        }



        // Validate indicator
        if (isEmpty(indicator)) {
            return new ValidationResult(301, "Missing indicator");
        }
        if (!ALLOWED_INDICATORS.contains(indicator)) {
            return new ValidationResult(302, "Invalid indicator: " + indicator +
                    ". Allowed values: " + String.join(", ", ALLOWED_INDICATORS));
        }

        // Validate based on operation type
        switch (indicator.toLowerCase()) {
            case "i": // Insert
                if (isEmpty(jsonPayload)) {
                    return new ValidationResult(301, "Missing jsonPayload for insert operation");
                }
                break;
            case "u": // Update
                if (isEmpty(id)) {
                    return new ValidationResult(301, "Missing ID for update operation");
                }
                if (isEmpty(jsonPayload)) {
                    return new ValidationResult(301, "Missing jsonPayload for update operation");
                }
                break;
            case "l": // Lookup
                // No additional checks required
                break;
            case "hof": // HOF Enrollment
                if (isEmpty(jsonPayload)) {
                    return new ValidationResult(301, "Missing jsonPayload for HOF enrollment operation");
                }
                break;
        }

        // Validate ID length if present
        if (!isEmpty(id) && id.length() > MAX_ID_LENGTH) {
            return new ValidationResult(302, "ID too long (max " + MAX_ID_LENGTH + " characters)");
        }

        // Validate jsonPayload length if present
        if (!isEmpty(jsonPayload) && jsonPayload.length() > MAX_JSON_PAYLOAD_LENGTH) {
            return new ValidationResult(302, "jsonPayload too long (max " + MAX_JSON_PAYLOAD_LENGTH + " characters)");
        }

        return new ValidationResult(200, "Valid request");
    }

    /**
     * Checks if a string is null or empty after trimming.
     */
    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    /**
     * Determines the type of operation.
     *
     * @return Operation type (INSERT, UPDATE, LOOKUP)
     */
    public String getOperationType() {
        if (indicator == null) {
            return "UNKNOWN";
        }

        switch (indicator.toLowerCase()) {
            case "i":
                return "INSERT";
            case "u":
                return "UPDATE";
            case "l":
                return "LOOKUP";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Determines whether the operation requires an ID.
     *
     * @return true if ID is required
     */
    public boolean requiresId() {
        return "u".equalsIgnoreCase(indicator);
    }

    /**
     * Determines whether the operation requires a JSON payload.
     *
     * @return true if jsonPayload is required
     */
    public boolean requiresPayload() {
        return "i".equalsIgnoreCase(indicator) || "u".equalsIgnoreCase(indicator);
    }

    @Override
    public String toString() {
        return "GenericRequest{" +
                "GGheader=" + GGheader +

                ", id='" + id + '\'' +
                ", indicator='" + indicator + '\'' +
                ", jsonPayload='" + jsonPayload + '\'' +
                '}';
    }

    /**
     * Inner class representing the validation result.
     */
    public static class ValidationResult {
        private final int code;
        private final String message;

        public ValidationResult(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        public boolean isValid() {
            return code == 200;
        }
    }
}
