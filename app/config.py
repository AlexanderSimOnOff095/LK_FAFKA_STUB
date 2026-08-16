import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://stub_kafka:stub_kafka_password@localhost:5432/lk_fafka_stub_kafka")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "applications.status")
STUB_CONSUMER_GROUP = os.getenv("STUB_CONSUMER_GROUP", "stub-status-change-v1")
ADMIN_PORT = int(os.getenv("ADMIN_PORT", "8080"))
