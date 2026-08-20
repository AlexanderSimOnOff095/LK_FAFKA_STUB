package ru.eapo.stub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public final class Repository {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> ALLOWED_SETTINGS = Set.of(
            "processing_mode",
            "result_publish_delay_ms",
            "error_simulation_enabled",
            "error_code",
            "error_message",
            "retryable"
    );

    private final JdbcTarget target;

    public Repository(String databaseUrl) {
        this.target = JdbcTarget.from(databaseUrl);
    }

    Connection connect() throws SQLException {
        return DriverManager.getConnection(target.url(), target.properties());
    }

    public Settings settings() throws SQLException {
        String sql = """
                SELECT processing_mode, result_publish_delay_ms, error_simulation_enabled,
                       error_code, error_message, retryable
                  FROM service_settings
                 WHERE id = 1
                """;
        try (Connection connection = connect();
             PreparedStatement query = connection.prepareStatement(sql);
             ResultSet result = query.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("service_settings row id=1 is missing");
            }
            return new Settings(
                    result.getString(1),
                    result.getInt(2),
                    result.getBoolean(3),
                    result.getString(4),
                    result.getString(5),
                    result.getBoolean(6)
            );
        }
    }

    public Map<String, String> transitions() throws SQLException {
        String sql = """
                SELECT incoming_status, target_status
                  FROM application_status_transition
                 WHERE is_active
                 ORDER BY priority, id
                """;
        Map<String, String> transitions = new LinkedHashMap<>();
        try (Connection connection = connect();
             PreparedStatement query = connection.prepareStatement(sql);
             ResultSet result = query.executeQuery()) {
            while (result.next()) {
                transitions.putIfAbsent(result.getString(1), result.getString(2));
            }
        }
        return transitions;
    }

    public ProcessedRequest findProcessed(String requestId) throws SQLException {
        String sql = "SELECT request_fingerprint, result_payload FROM processed_request WHERE request_id = ?";
        try (Connection connection = connect(); PreparedStatement query = connection.prepareStatement(sql)) {
            query.setString(1, requestId);
            try (ResultSet result = query.executeQuery()) {
                if (!result.next()) {
                    return null;
                }
                try {
                    return new ProcessedRequest(result.getString(1), JSON.readTree(result.getString(2)));
                } catch (JsonProcessingException exception) {
                    throw new SQLException("Stored result_payload is not valid JSON", exception);
                }
            }
        }
    }

    public void saveProcessed(String requestId, String fingerprint, JsonNode payload) throws SQLException {
        String sql = """
                INSERT INTO processed_request(request_id, request_fingerprint, result_payload)
                VALUES (?, ?, ?::jsonb)
                ON CONFLICT(request_id) DO NOTHING
                """;
        try (Connection connection = connect(); PreparedStatement query = connection.prepareStatement(sql)) {
            query.setString(1, requestId);
            query.setString(2, fingerprint);
            query.setString(3, payload.toString());
            query.executeUpdate();
        }
    }

    public void saveTransition(String current, String next) throws SQLException {
        String deleteSql = "DELETE FROM application_status_transition WHERE incoming_status = ?";
        String insertSql = """
                INSERT INTO application_status_transition(
                    application_id, application_type, incoming_status, target_status
                ) VALUES ('*', 'DEFAULT', ?, ?)
                """;
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try (PreparedStatement delete = connection.prepareStatement(deleteSql);
                 PreparedStatement insert = connection.prepareStatement(insertSql)) {
                delete.setString(1, current);
                delete.executeUpdate();
                insert.setString(1, current);
                insert.setString(2, next);
                insert.executeUpdate();
                connection.commit();
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    public void deleteTransition(String current) throws SQLException {
        try (Connection connection = connect();
             PreparedStatement query = connection.prepareStatement(
                     "DELETE FROM application_status_transition WHERE incoming_status = ?")) {
            query.setString(1, current);
            query.executeUpdate();
        }
    }

    public void updateSettings(Map<String, Object> values) throws SQLException {
        LinkedHashMap<String, Object> accepted = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            if (ALLOWED_SETTINGS.contains(key)) {
                accepted.put(key, value);
            }
        });
        if (accepted.isEmpty()) {
            return;
        }

        String assignments = String.join(", ", accepted.keySet().stream().map(key -> key + " = ?").toList());
        String sql = "UPDATE service_settings SET " + assignments + ", updated_at = now() WHERE id = 1";
        try (Connection connection = connect(); PreparedStatement query = connection.prepareStatement(sql)) {
            int index = 1;
            for (Object value : accepted.values()) {
                query.setObject(index++, value);
            }
            query.executeUpdate();
        }
    }

    public void reset() throws SQLException {
        String resetSettings = """
                UPDATE service_settings
                   SET processing_mode = 'STATUS_TRANSITION',
                       result_publish_delay_ms = 0,
                       error_simulation_enabled = false,
                       error_code = 'INTERNAL_ERROR',
                       error_message = 'Test processing error',
                       retryable = false,
                       updated_at = now()
                 WHERE id = 1
                """;
        String resetTransitions = """
                INSERT INTO application_status_transition(
                    application_id, application_type, incoming_status, target_status, priority
                ) VALUES
                    ('*', 'DEFAULT', 'NEW', 'PROCESSING', 1),
                    ('*', 'DEFAULT', 'PROCESSING', 'COMPLETED', 1),
                    ('*', 'DEFAULT', 'COMPLETED', 'COMPLETED', 1),
                    ('*', 'DEFAULT', 'ERROR', 'PROCESSING', 1)
                """;
        try (Connection connection = connect()) {
            connection.setAutoCommit(false);
            try (Statement query = connection.createStatement()) {
                query.executeUpdate(resetSettings);
                query.executeUpdate("DELETE FROM application_status_transition");
                query.executeUpdate(resetTransitions);
                connection.commit();
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    public record ProcessedRequest(String fingerprint, JsonNode payload) {
    }

    private record JdbcTarget(String url, Properties properties) {
        private static JdbcTarget from(String databaseUrl) {
            String withoutJdbc = databaseUrl.startsWith("jdbc:") ? databaseUrl.substring(5) : databaseUrl;
            URI uri = URI.create(withoutJdbc);
            if (!"postgresql".equals(uri.getScheme())) {
                throw new IllegalArgumentException("DATABASE_URL must use the postgresql scheme");
            }

            StringBuilder jdbc = new StringBuilder("jdbc:postgresql://")
                    .append(uri.getHost())
                    .append(':')
                    .append(uri.getPort() == -1 ? 5432 : uri.getPort())
                    .append(uri.getRawPath());
            if (uri.getRawQuery() != null) {
                jdbc.append('?').append(uri.getRawQuery());
            }

            Properties properties = new Properties();
            if (uri.getRawUserInfo() != null) {
                String[] userInfo = uri.getRawUserInfo().split(":", 2);
                properties.setProperty("user", URLDecoder.decode(userInfo[0], StandardCharsets.UTF_8));
                if (userInfo.length == 2) {
                    properties.setProperty("password", URLDecoder.decode(userInfo[1], StandardCharsets.UTF_8));
                }
            }
            return new JdbcTarget(jdbc.toString(), properties);
        }
    }
}
