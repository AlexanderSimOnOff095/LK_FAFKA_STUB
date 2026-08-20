package ru.eapo.stub;

public record AppConfig(
        String databaseUrl,
        String kafkaBootstrapServers,
        String kafkaTopic,
        String stubConsumerGroup,
        int adminPort
) {
    public static AppConfig fromEnvironment() {
        return new AppConfig(
                env("DATABASE_URL", "postgresql://stub_kafka:stub_kafka_password@localhost:5432/lk_fafka_stub_kafka"),
                env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env("KAFKA_TOPIC", "applications.status"),
                env("STUB_CONSUMER_GROUP", "stub-status-change-v1"),
                Integer.parseInt(env("ADMIN_PORT", "8080"))
        );
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
