package ru.eapo.stub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DomainTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void choosesConfiguredTransition() {
        assertEquals("PROCESSING", Domain.chooseStatus("NEW", new Settings(), Map.of("NEW", "PROCESSING")));
    }

    @Test
    void alwaysCompletedModeIgnoresTransitions() {
        Settings settings = new Settings("ALWAYS_COMPLETED", 0, false, "INTERNAL_ERROR", "error", false);
        assertEquals("COMPLETED", Domain.chooseStatus("NEW", settings, Map.of()));
    }

    @Test
    void keepCurrentModeReturnsCurrentStatus() {
        Settings settings = new Settings("KEEP_CURRENT_STATUS", 0, false, "INTERNAL_ERROR", "error", false);
        assertEquals("NEW", Domain.chooseStatus("NEW", settings, Map.of()));
    }

    @Test
    void unknownStatusFails() {
        assertThrows(NoSuchElementException.class, () -> Domain.chooseStatus("UNKNOWN", new Settings(), Map.of()));
    }

    @Test
    void validatesRequest() {
        assertDoesNotThrow(() -> Domain.validateRequest(request(), "a1"));
    }

    @Test
    void rejectsMismatchedKafkaKey() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Domain.validateRequest(request(), "a2")
        );
        assertEquals("APPLICATION_KEY_MISMATCH", exception.getMessage());
    }

    @Test
    void decodesRestProxyJsonKey() {
        assertEquals("a1", Domain.decodeMessageKey("\"a1\"".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void decodesPlainKafkaKey() {
        assertEquals("a1", Domain.decodeMessageKey("a1".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void restProxyKeyRoundTrip() {
        assertEquals("a1", Domain.decodeMessageKey(Domain.encodeMessageKey("a1")));
    }

    @Test
    void fingerprintDependsOnlyOnBusinessPayload() {
        ObjectNode first = request();
        ObjectNode second = request();
        first.put("eventId", "x");
        second.put("eventId", "y");
        assertEquals(Domain.fingerprint(first), Domain.fingerprint(second));
        assertEquals("e9fb543d2b2c76f84234c1fb68d9502a131c259553a95df35dfce8eaa183705f", Domain.fingerprint(first));
    }

    @Test
    void buildsSuccessResult() {
        ObjectNode result = Domain.buildResult(request(), "PROCESSING", null);
        assertEquals("SUCCESS", result.path("result").asText());
        assertEquals("e1", result.path("causationId").asText());
    }

    @Test
    void buildsErrorResult() {
        ObjectNode result = Domain.buildResult(
                request(),
                null,
                new Domain.ProcessingError("UNKNOWN_STATUS", "missing", false)
        );
        assertEquals("ERROR", result.path("result").asText());
        assertNull(result.get("status").textValue());
    }

    private static ObjectNode request() {
        ObjectNode event = JSON.createObjectNode();
        event.put("eventId", "e1");
        event.put("eventType", Domain.REQUEST_TYPE);
        event.put("eventVersion", 1);
        event.put("requestId", "r1");
        event.put("correlationId", "c1");
        event.put("producer", "eapo-cab");
        event.put("applicationId", "a1");
        event.put("currentStatus", "NEW");
        return event;
    }
}
