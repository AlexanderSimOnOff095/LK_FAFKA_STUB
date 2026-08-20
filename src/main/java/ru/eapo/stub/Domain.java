package ru.eapo.stub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

public final class Domain {
    public static final String REQUEST_TYPE = "APPLICATION_STATUS_CHANGE_REQUESTED";
    public static final String RESULT_TYPE = "APPLICATION_STATUS_CHANGE_RESULT";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ObjectMapper ASCII_JSON = new ObjectMapper(
            com.fasterxml.jackson.core.JsonFactory.builder()
                    .configure(JsonWriteFeature.ESCAPE_NON_ASCII, true)
                    .build()
    );

    private Domain() {
    }

    public static String decodeMessageKey(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return "";
        }
        String text = new String(raw, StandardCharsets.UTF_8);
        try {
            JsonNode decoded = JSON.readTree(text);
            return decoded != null && decoded.isTextual() ? decoded.textValue() : text;
        } catch (JsonProcessingException ignored) {
            return text;
        }
    }

    public static byte[] encodeMessageKey(String value) {
        try {
            return JSON.writeValueAsBytes(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Cannot encode Kafka key", exception);
        }
    }

    public static String fingerprint(JsonNode event) {
        try {
            String applicationId = ASCII_JSON.writeValueAsString(valueOrNull(event, "applicationId"));
            String currentStatus = ASCII_JSON.writeValueAsString(valueOrNull(event, "currentStatus"));
            String body = "{\"applicationId\": " + applicationId + ", \"currentStatus\": " + currentStatus + "}";
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (JsonProcessingException | NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Cannot fingerprint request", exception);
        }
    }

    public static void validateRequest(JsonNode event, String messageKey) {
        String[] required = {"eventId", "requestId", "correlationId", "applicationId", "currentStatus"};
        StringBuilder missing = new StringBuilder();
        for (String name : required) {
            JsonNode value = event.get(name);
            if (value == null || value.isNull() || (value.isTextual() && value.textValue().isEmpty()) || value.isBoolean() && !value.booleanValue() || value.isNumber() && value.asDouble() == 0) {
                if (!missing.isEmpty()) {
                    missing.append(',');
                }
                missing.append(name);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("INVALID_MESSAGE:" + missing);
        }
        if (!REQUEST_TYPE.equals(event.path("eventType").asText()) || !"eapo-cab".equals(event.path("producer").asText())) {
            throw new IllegalArgumentException("UNSUPPORTED_EVENT");
        }
        if (event.path("eventVersion").asInt(Integer.MIN_VALUE) != 1) {
            throw new IllegalArgumentException("UNSUPPORTED_EVENT_VERSION");
        }
        if (!event.path("applicationId").asText().equals(messageKey)) {
            throw new IllegalArgumentException("APPLICATION_KEY_MISMATCH");
        }
    }

    public static ObjectNode buildResult(JsonNode event, String nextStatus, ProcessingError error) {
        ObjectNode result = JSON.createObjectNode();
        result.put("eventId", UUID.randomUUID().toString());
        result.put("eventType", RESULT_TYPE);
        result.put("eventVersion", 1);
        result.put("occurredAt", Instant.now().toString());
        copyOrNull(event, result, "requestId");
        if (event.hasNonNull("correlationId") && !event.path("correlationId").asText().isEmpty()) {
            result.set("correlationId", event.get("correlationId"));
        } else {
            result.set("correlationId", valueOrNull(event, "requestId"));
        }
        result.set("causationId", valueOrNull(event, "eventId"));
        result.put("producer", "status-stub");
        copyOrNull(event, result, "applicationId");
        result.set("previousStatus", valueOrNull(event, "currentStatus"));
        if (nextStatus == null) {
            result.putNull("status");
        } else {
            result.put("status", nextStatus);
        }
        result.put("result", error == null ? "SUCCESS" : "ERROR");
        if (error != null) {
            result.put("errorCode", error.code());
            result.put("message", error.message());
            result.put("retryable", error.retryable());
        }
        return result;
    }

    public static String chooseStatus(String current, Settings settings, Map<String, String> transitions) {
        return switch (settings.processingMode()) {
            case "ALWAYS_COMPLETED" -> "COMPLETED";
            case "KEEP_CURRENT_STATUS" -> current;
            default -> {
                String next = transitions.get(current);
                if (next == null) {
                    throw new NoSuchElementException("UNKNOWN_STATUS");
                }
                yield next;
            }
        };
    }

    private static JsonNode valueOrNull(JsonNode source, String field) {
        JsonNode value = source.get(field);
        return value == null ? JSON.nullNode() : value;
    }

    private static void copyOrNull(JsonNode source, ObjectNode target, String field) {
        target.set(field, valueOrNull(source, field));
    }

    public record ProcessingError(String code, String message, boolean retryable) {
    }
}
