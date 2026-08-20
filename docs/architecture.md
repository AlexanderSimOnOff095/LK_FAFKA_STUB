# Архитектура

`EAPO-Cab -> applications.status -> STUB -> applications.status -> EAPO-Cab`

- Реализация: Java 21, Kafka Java client, PostgreSQL JDBC, Jackson.
- Kafka key: `applicationId`.
- REQUEST: `APPLICATION_STATUS_CHANGE_REQUESTED`, producer `eapo-cab`.
- RESULT: `APPLICATION_STATUS_CHANGE_RESULT`, producer `status-stub`.
- Доставка: at-least-once.
- Идемпотентность: `requestId` + fingerprint payload.
- PostgreSQL: `lk_fafka_stub_kafka`; переходы, настройки, дедупликация.
- Административный REST API не используется для бизнес-обмена с ЛК.
- Конфигурация контейнеров: `compose.json`; OpenAPI: `src/main/resources/openapi.json`.
