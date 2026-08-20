package ru.eapo.stub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class AdminServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AdminServer.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Repository repository;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final byte[] openApi;
    private final byte[] swaggerHtml;

    public AdminServer(int port, Repository repository) throws IOException {
        this.repository = repository;
        this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        this.server.setExecutor(executor);
        this.server.createContext("/", this::handle);
        this.openApi = resource("/openapi.json");
        this.swaggerHtml = resource("/swagger.html");
    }

    public void start() {
        server.start();
        LOG.info("Admin API listening on port {}", server.getAddress().getPort());
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();

            if ("GET".equals(method) && "/".equals(path)) {
                exchange.getResponseHeaders().set("Location", "/swagger");
                exchange.sendResponseHeaders(302, -1);
                return;
            }
            if ("GET".equals(method) && ("/swagger".equals(path) || "/swagger/".equals(path))) {
                send(exchange, 200, "text/html; charset=utf-8", swaggerHtml);
                return;
            }
            if ("GET".equals(method) && "/openapi.json".equals(path)) {
                send(exchange, 200, "application/json; charset=utf-8", openApi);
                return;
            }
            if ("GET".equals(method) && "/api/v1/admin/health".equals(path)) {
                health(exchange);
                return;
            }
            if ("GET".equals(method) && "/api/v1/admin/settings".equals(path)) {
                sendJson(exchange, 200, settingsResponse());
                return;
            }
            if ("GET".equals(method) && "/api/v1/admin/status-transitions".equals(path)) {
                sendJson(exchange, 200, transitionsResponse());
                return;
            }
            if ("PUT".equals(method) && "/api/v1/admin/settings/processing-mode".equals(path)) {
                JsonNode body = body(exchange);
                String processingMode = requiredText(body, "processingMode");
                repository.updateSettings(Map.of("processing_mode", processingMode));
                ObjectNode response = JSON.createObjectNode();
                response.put("processingMode", processingMode);
                response.put("result", "SUCCESS");
                sendJson(exchange, 200, response);
                return;
            }
            if ("PUT".equals(method) && "/api/v1/admin/settings/response-delay".equals(path)) {
                JsonNode body = body(exchange);
                int delay = requiredInt(body, "resultPublishDelayMs");
                repository.updateSettings(Map.of("result_publish_delay_ms", delay));
                ObjectNode response = JSON.createObjectNode();
                response.put("resultPublishDelayMs", delay);
                response.put("result", "SUCCESS");
                sendJson(exchange, 200, response);
                return;
            }
            if ("PUT".equals(method) && "/api/v1/admin/error-simulation".equals(path)) {
                JsonNode body = body(exchange);
                boolean enabled = body.path("enabled").asBoolean(false);
                Map<String, Object> values = new LinkedHashMap<>();
                values.put("error_simulation_enabled", enabled);
                values.put("error_code", textOr(body, "errorCode", "INTERNAL_ERROR"));
                values.put("error_message", textOr(body, "message", "Test processing error"));
                values.put("retryable", body.path("retryable").asBoolean(false));
                repository.updateSettings(values);
                ObjectNode response = JSON.createObjectNode();
                response.put("enabled", enabled);
                response.put("result", "SUCCESS");
                sendJson(exchange, 200, response);
                return;
            }
            if ("POST".equals(method) && "/api/v1/admin/settings/reset".equals(path)) {
                repository.reset();
                ObjectNode response = JSON.createObjectNode();
                response.put("result", "SUCCESS");
                response.put("message", "Настройки восстановлены");
                sendJson(exchange, 200, response);
                return;
            }
            if (path.startsWith("/api/v1/admin/status-transitions/")) {
                String current = URLDecoder.decode(
                        path.substring("/api/v1/admin/status-transitions/".length()),
                        StandardCharsets.UTF_8
                );
                if ("PUT".equals(method)) {
                    String next = requiredText(body(exchange), "nextStatus");
                    repository.saveTransition(current, next);
                    ObjectNode response = JSON.createObjectNode();
                    response.put("currentStatus", current);
                    response.put("nextStatus", next);
                    response.put("result", "SUCCESS");
                    sendJson(exchange, 200, response);
                    return;
                }
                if ("DELETE".equals(method)) {
                    repository.deleteTransition(current);
                    send(exchange, 204, "application/json; charset=utf-8", new byte[0]);
                    return;
                }
            }

            ObjectNode notFound = JSON.createObjectNode();
            notFound.put("errorCode", "NOT_FOUND");
            sendJson(exchange, 404, notFound);
        } catch (IllegalArgumentException exception) {
            ObjectNode error = JSON.createObjectNode();
            error.put("errorCode", "INVALID_REQUEST");
            error.put("message", exception.getMessage());
            sendJson(exchange, 400, error);
        } catch (Exception exception) {
            LOG.error("Admin request failed", exception);
            ObjectNode error = JSON.createObjectNode();
            error.put("errorCode", "INTERNAL_ERROR");
            error.put("message", exception.getMessage());
            sendJson(exchange, 500, error);
        } finally {
            exchange.close();
        }
    }

    private void health(HttpExchange exchange) throws IOException {
        try {
            repository.settings();
            ObjectNode result = JSON.createObjectNode();
            result.put("status", "UP");
            result.put("kafkaConnected", true);
            result.put("configurationLoaded", true);
            sendJson(exchange, 200, result);
        } catch (SQLException exception) {
            ObjectNode result = JSON.createObjectNode();
            result.put("status", "DOWN");
            result.put("message", exception.getMessage());
            sendJson(exchange, 503, result);
        }
    }

    private ObjectNode settingsResponse() throws SQLException {
        Settings settings = repository.settings();
        ObjectNode response = JSON.createObjectNode();
        response.put("processingMode", settings.processingMode());
        response.put("resultPublishDelayMs", settings.resultPublishDelayMs());
        response.set("transitions", JSON.valueToTree(repository.transitions()));
        ObjectNode errorSimulation = response.putObject("errorSimulation");
        errorSimulation.put("enabled", settings.errorSimulationEnabled());
        errorSimulation.put("errorCode", settings.errorCode());
        errorSimulation.put("retryable", settings.retryable());
        return response;
    }

    private ObjectNode transitionsResponse() throws SQLException {
        ArrayNode transitions = JSON.createArrayNode();
        repository.transitions().forEach((current, next) -> {
            ObjectNode transition = transitions.addObject();
            transition.put("currentStatus", current);
            transition.put("nextStatus", next);
        });
        ObjectNode response = JSON.createObjectNode();
        response.set("transitions", transitions);
        return response;
    }

    private static JsonNode body(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readAllBytes();
        return bytes.length == 0 ? JSON.createObjectNode() : JSON.readTree(bytes);
    }

    private static String requiredText(JsonNode body, String name) {
        if (!body.hasNonNull(name) || body.path(name).asText().isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return body.path(name).asText();
    }

    private static int requiredInt(JsonNode body, String name) {
        if (!body.has(name) || !body.path(name).canConvertToInt()) {
            throw new IllegalArgumentException(name + " must be an integer");
        }
        return body.path(name).intValue();
    }

    private static String textOr(JsonNode body, String name, String fallback) {
        return body.hasNonNull(name) ? body.path(name).asText() : fallback;
    }

    private static void sendJson(HttpExchange exchange, int status, JsonNode data) throws IOException {
        send(exchange, status, "application/json; charset=utf-8", JSON.writeValueAsBytes(data));
    }

    private static void send(HttpExchange exchange, int status, String contentType, byte[] payload) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        if (status == 204) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, payload.length);
        exchange.getResponseBody().write(payload);
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream stream = AdminServer.class.getResourceAsStream(name)) {
            if (stream == null) {
                throw new IOException("Resource not found: " + name);
            }
            return stream.readAllBytes();
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdown();
    }
}
