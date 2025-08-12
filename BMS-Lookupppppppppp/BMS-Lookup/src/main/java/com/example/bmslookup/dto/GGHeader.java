package com.example.bmslookup.dto;

import javax.xml.bind.annotation.*;
import java.util.Arrays;
import java.util.List;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GGHeader", propOrder = {
    "correlationId",
    "originatingChannel",
    "channelRequestId",
    "originatingUserType",
    "originatingUserIdentifier",
    "serviceSlug",
    "serviceEntityId"
})
@XmlRootElement(name = "GGheader", namespace = "http://teradata.com/uhi")
public class GGHeader {
       private static final List<String> ALLOWED_CHANNELS = Arrays.asList(
        "16", "10"
    );

    private static final List<String> ALLOWED_USER_TYPES = Arrays.asList(
        "1", "2"
    );

    private static final List<String> ALLOWED_SERVICE_SLUGS = Arrays.asList(
        "BMS-LOOKUP-01", "BMS-LOOKUP-02", "BMS-LOOKUP-03"
    );

    private static final List<String> ALLOWED_ENTITY_IDS = Arrays.asList(
        "1", "2"
    );
    private static final List<String> ALLOWED_CORRELATION_IDS = Arrays.asList("1", "2", "3");
    private static final List<String> ALLOWED_CHANNEL_REQUEST_IDS = Arrays.asList("1", "2", "3");
    private static final List<String> ALLOWED_USER_IDENTIFIERS = Arrays.asList("2970430001808");


    private static final int MAX_CORRELATION_ID_LENGTH = 50;
    private static final int MAX_CHANNEL_REQUEST_ID_LENGTH = 50;
    private static final int MAX_USER_IDENTIFIER_LENGTH = 100;
    private static final int MAX_SERVICE_SLUG_LENGTH = 50;
    private static final int MAX_SERVICE_ENTITY_ID_LENGTH = 100;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String correlationId;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String originatingChannel;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String channelRequestId;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String originatingUserType;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String originatingUserIdentifier;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String serviceSlug;

    @XmlElement(required = true, namespace = "http://teradata.com/uhi")
    private String serviceEntityId;

    // Constructors
    public GGHeader() {}

    public GGHeader(String correlationId, String originatingChannel, String channelRequestId,
                   String originatingUserType, String originatingUserIdentifier,
                   String serviceSlug, String serviceEntityId) {
        this.correlationId = correlationId;
        this.originatingChannel = originatingChannel;
        this.channelRequestId = channelRequestId;
        this.originatingUserType = originatingUserType;
        this.originatingUserIdentifier = originatingUserIdentifier;
        this.serviceSlug = serviceSlug;
        this.serviceEntityId = serviceEntityId;
    }

    // Getters and Setters
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getOriginatingChannel() {
        return originatingChannel;
    }

    public void setOriginatingChannel(String originatingChannel) {
        this.originatingChannel = originatingChannel;
    }

    public String getChannelRequestId() {
        return channelRequestId;
    }

    public void setChannelRequestId(String channelRequestId) {
        this.channelRequestId = channelRequestId;
    }

    public String getOriginatingUserType() {
        return originatingUserType;
    }

    public void setOriginatingUserType(String originatingUserType) {
        this.originatingUserType = originatingUserType;
    }

    public String getOriginatingUserIdentifier() {
        return originatingUserIdentifier;
    }

    public void setOriginatingUserIdentifier(String originatingUserIdentifier) {
        this.originatingUserIdentifier = originatingUserIdentifier;
    }

    public String getServiceSlug() {
        return serviceSlug;
    }

    public void setServiceSlug(String serviceSlug) {
        this.serviceSlug = serviceSlug;
    }

    public String getServiceEntityId() {
        return serviceEntityId;
    }

    public void setServiceEntityId(String serviceEntityId) {
        this.serviceEntityId = serviceEntityId;
    }

    /**
     * checks the validity of the GGHeader fields.
     * 
     * @return ValidationResult 
     */
    public ValidationResult validate() {
        if (isEmpty(correlationId)) {
            return new ValidationResult(301, "Missing correlationId");
        }
        if (!ALLOWED_CORRELATION_IDS.contains(correlationId)) {
            return new ValidationResult(302, "Invalid correlationId: " + correlationId);
        }

        if (isEmpty(originatingChannel)) {
            return new ValidationResult(301, "Missing originatingChannel");
        }
        if (!ALLOWED_CHANNELS.contains(originatingChannel)) {
            return new ValidationResult(302, "Invalid originatingChannel: " + originatingChannel);
        }

        if (isEmpty(channelRequestId)) {
            return new ValidationResult(301, "Missing channelRequestId");
        }
        if (!ALLOWED_CHANNEL_REQUEST_IDS.contains(channelRequestId)) {
            return new ValidationResult(302, "Invalid channelRequestId: " + channelRequestId);
        }

        if (isEmpty(originatingUserType)) {
            return new ValidationResult(301, "Missing originatingUserType");
        }
        if (!ALLOWED_USER_TYPES.contains(originatingUserType)) {
            return new ValidationResult(302, "Invalid originatingUserType: " + originatingUserType);
        }

        if (isEmpty(originatingUserIdentifier)) {
            return new ValidationResult(301, "Missing originatingUserIdentifier");
        }
        if (!ALLOWED_USER_IDENTIFIERS.contains(originatingUserIdentifier)) {
            return new ValidationResult(302, "Invalid originatingUserIdentifier: " + originatingUserIdentifier);
        }

        if (isEmpty(serviceSlug)) {
            return new ValidationResult(301, "Missing serviceSlug");
        }
        if (!ALLOWED_SERVICE_SLUGS.contains(serviceSlug)) {
            return new ValidationResult(302, "Invalid serviceSlug: " + serviceSlug);
        }

        if (isEmpty(serviceEntityId)) {
            return new ValidationResult(301, "Missing serviceEntityId");
        }
        if (!ALLOWED_ENTITY_IDS.contains(serviceEntityId)) {
            return new ValidationResult(302, "Invalid serviceEntityId: " + serviceEntityId);
        }

        return new ValidationResult(200, "Valid header");
    }

    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    @Override
    public String toString() {
        return "GGHeader{" +
                "correlationId='" + correlationId + '\'' +
                ", originatingChannel='" + originatingChannel + '\'' +
                ", channelRequestId='" + channelRequestId + '\'' +
                ", originatingUserType='" + originatingUserType + '\'' +
                ", originatingUserIdentifier='" + originatingUserIdentifier + '\'' +
                ", serviceSlug='" + serviceSlug + '\'' +
                ", serviceEntityId='" + serviceEntityId + '\'' +
                '}';
    }

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
