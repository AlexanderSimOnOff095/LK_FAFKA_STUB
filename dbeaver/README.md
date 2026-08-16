# DBeaver

Файл `STUB_Kafka.dbeaver-data-sources.json` содержит подключение к PostgreSQL проекта:

- Host: `localhost`
- Port: `5433`
- Database: `lk_fafka_stub_kafka`
- User: `stub_kafka`
- Password: значение `POSTGRES_PASSWORD` из `.env`

Пароль намеренно не сохранен в Git. Конфигурацию можно импортировать в DBeaver либо создать подключение вручную по указанным параметрам. После подключения доступны таблицы `application_status_transition`, `service_settings` и `processed_request`.
