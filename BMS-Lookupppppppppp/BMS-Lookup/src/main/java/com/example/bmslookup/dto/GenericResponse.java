package com.example.bmslookup.dto;

import javax.xml.bind.annotation.*;
import javax.xml.datatype.XMLGregorianCalendar;

/**
 * DTO for a generic response containing status code, message, data, and timestamp
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GenericResponse", propOrder = {
    "responseCode",
    "responseMessage",
    "data",
    "timestamp"
})
@XmlRootElement(name = "GenericResponse", namespace = "http://teradata.com/uhi")
public class GenericResponse {

    // Defined response codes
    public static final int SUCCESS = 200;
    public static final int MISSING_REQUIRED_FIELD = 301;
    public static final int INVALID_VALUE = 302;
    public static final int BAD_REQUEST = 400;
    public static final int NOT_FOUND = 404;
    public static final int INTERNAL_SERVER_ERROR = 500;

    // Default response messages
    public static final String SUCCESS_MESSAGE = "Operation completed successfully";
    public static final String MISSING_FIELD_MESSAGE = "Missing required field";
    public static final String INVALID_VALUE_MESSAGE = "Invalid value provided";
    public static final String BAD_REQUEST_MESSAGE = "Bad request";
    public static final String NOT_FOUND_MESSAGE = "Record not found";
    public static final String INTERNAL_ERROR_MESSAGE = "Internal server error";

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private int responseCode;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String responseMessage;

    @XmlElement(namespace = "http://teradata.com/uhi")
    private String data;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private XMLGregorianCalendar timestamp;

    // Constructors
    public GenericResponse() {}

    public GenericResponse(int responseCode, String responseMessage) {
        this.responseCode = responseCode;
        this.responseMessage = responseMessage;
    }

    public GenericResponse(int responseCode, String responseMessage, String data) {
        this.responseCode = responseCode;
        this.responseMessage = responseMessage;
        this.data = data;
    }

    public GenericResponse(int responseCode, String responseMessage, String data, XMLGregorianCalendar timestamp) {
        this.responseCode = responseCode;
        this.responseMessage = responseMessage;
        this.data = data;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getResponseMessage() {
        return responseMessage;
    }

    public void setResponseMessage(String responseMessage) {
        this.responseMessage = responseMessage;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public XMLGregorianCalendar getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(XMLGregorianCalendar timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Check if the response is successful
     *
     * @return true if the response code is 200
     */
    public boolean isSuccess() {
        return responseCode == SUCCESS;
    }

    /**
     * Check if the response indicates an error
     *
     * @return true if the response code is not 200
     */
    public boolean isError() {
        return responseCode != SUCCESS;
    }

    /**
     * Get the type of error
     *
     * @return error type as String
     */
    public String getErrorType() {
        switch (responseCode) {
            case MISSING_REQUIRED_FIELD:
                return "MISSING_REQUIRED_FIELD";
            case INVALID_VALUE:
                return "INVALID_VALUE";
            case BAD_REQUEST:
                return "BAD_REQUEST";
            case NOT_FOUND:
                return "NOT_FOUND";
            case INTERNAL_SERVER_ERROR:
                return "INTERNAL_SERVER_ERROR";
            default:
                return "UNKNOWN_ERROR";
        }
    }

    /**
     * Create a success response
     *
     * @param message success message
     * @return new GenericResponse
     */
    public static GenericResponse success(String message) {
        return new GenericResponse(SUCCESS, message != null ? message : SUCCESS_MESSAGE);
    }

    /**
     * Create a success response with data
     *
     * @param message success message
     * @param data response data
     * @return new GenericResponse
     */
    public static GenericResponse successWithData(String message, String data) {
        return new GenericResponse(SUCCESS, message != null ? message : SUCCESS_MESSAGE, data);
    }

    /**
     * Create an error response
     *
     * @param code error code
     * @param message error message
     * @return new GenericResponse
     */
    public static GenericResponse error(int code, String message) {
        return new GenericResponse(code, message);
    }

    /**
     * Create an error response with default message
     *
     * @param code error code
     * @return new GenericResponse
     */
    public static GenericResponse error(int code) {
        String defaultMessage;
        switch (code) {
            case MISSING_REQUIRED_FIELD:
                defaultMessage = MISSING_FIELD_MESSAGE;
                break;
            case INVALID_VALUE:
                defaultMessage = INVALID_VALUE_MESSAGE;
                break;
            case BAD_REQUEST:
                defaultMessage = BAD_REQUEST_MESSAGE;
                break;
            case NOT_FOUND:
                defaultMessage = NOT_FOUND_MESSAGE;
                break;
            case INTERNAL_SERVER_ERROR:
                defaultMessage = INTERNAL_ERROR_MESSAGE;
                break;
            default:
                defaultMessage = "Unknown error";
        }
        return new GenericResponse(code, defaultMessage);
    }

    /**
     * Create an error response for missing required field
     *
     * @param fieldName name of the missing field
     * @return new GenericResponse
     */
    public static GenericResponse missingField(String fieldName) {
        return new GenericResponse(MISSING_REQUIRED_FIELD,
            "Missing required field: " + fieldName);
    }

    /**
     * Create an error response for invalid value
     *
     * @param fieldName name of the field
     * @param value invalid value
     * @return new GenericResponse
     */
    public static GenericResponse invalidValue(String fieldName, String value) {
        return new GenericResponse(INVALID_VALUE,
            "Invalid value for field '" + fieldName + "': " + value);
    }

    @Override
    public String toString() {
        return "GenericResponse{" +
                "responseCode=" + responseCode +
                ", responseMessage='" + responseMessage + '\'' +
                ", data='" + data + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    /**
     * Compare this response with another one
     *
     * @param other the other response
     * @return true if both responses are equal
     */
    public boolean equals(GenericResponse other) {
        if (this == other) return true;
        if (other == null) return false;

        return responseCode == other.responseCode &&
               (responseMessage == null ? other.responseMessage == null :
                responseMessage.equals(other.responseMessage)) &&
               (data == null ? other.data == null : data.equals(other.data));
    }
}
