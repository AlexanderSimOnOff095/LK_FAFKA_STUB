# LK_FAFKA_STUB

Java 21 проект `STUB` для тестирования асинхронной смены статусов заявок EAPO-Cab через один Kafka-топик.

## Состав

- `applications.status` — единый топик REQUEST/RESULT;
- `stub-status-change-v1` — consumer group сервиса;
- `eapo-cab-status-result-v1` — consumer group Личного кабинета;
- PostgreSQL БД `lk_fafka_stub_kafka`;
- административный REST API на `http://localhost:8090`;
- Swagger UI на `http://localhost:8090/swagger` и OpenAPI JSON на `http://localhost:8090/openapi.json`;
- Kafka UI на `http://localhost:8081`;
- Kafka REST Proxy на `http://localhost:8082` для Postman;
- три Postman-коллекции в `postman/`.

## Запуск

1. Установить и запустить Docker Desktop.
2. Скопировать `.env.example` в `.env`.
3. Выполнить:

```powershell
docker compose -f compose.json up -d --build
```

Топик и БД создаются автоматически. Проверка:

```powershell
docker compose -f compose.json ps
docker compose -f compose.json exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic applications.status
Invoke-RestMethod http://localhost:8090/api/v1/admin/health
```

## Postman

Импортировать окружение `postman/LK_FAFKA_STUB.local.postman_environment.json` и коллекции:

- `postman/STUB_Admin.postman_collection.json` — настройка STUB;
- `postman/EAPO_Cab_Kafka_Status.postman_collection.json` — отправка REQUEST и чтение сообщений через Kafka REST Proxy.
- `postman/EAPO_Cab_Application_Status_E2E.postman_collection.json` — полный автоматизированный сценарий одной заявки `NEW -> PROCESSING -> COMPLETED`.

Postman не использует бинарный Kafka-протокол напрямую. Для тестов коллекция обращается к REST Proxy на порту `8082`; Kafka UI на `8081` используется для просмотра топика и сообщений.

## DBeaver

Создать PostgreSQL connection:

- Host: `localhost`
- Port: `5433`
- Database: `lk_fafka_stub_kafka`
- User: `stub_kafka`
- Password: значение `POSTGRES_PASSWORD` из `.env`

DDL: `db/init/001_schema.sql`. Все имена баз данных проекта имеют суффикс `_kafka`.

Импортируемая конфигурация DBeaver: `dbeaver/STUB_Kafka.dbeaver-data-sources.json`. Пароль в Git не сохраняется.

Порты `5433` и `8090` выбраны намеренно, чтобы не конфликтовать с ранее запущенными PostgreSQL и REST STUB на `5432`/`8080`.

## Тесты

Для локальной сборки нужны JDK 21 и Maven 3.9+. Автономные unit-тесты не требуют Kafka/PostgreSQL:

```powershell
mvn test
```

Сборка исполняемого JAR:

```powershell
mvn package
java -jar target/lk-fafka-stub.jar
```

Полная проверка после запуска Docker:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1
```

## Контракт

STUB читает только `APPLICATION_STATUS_CHANGE_REQUESTED` от `producer=eapo-cab`. Результат публикуется как `APPLICATION_STATUS_CHANGE_RESULT` от `producer=status-stub` с тем же Kafka key (`applicationId`). Offset подтверждается после публикации результата; повторный `requestId` обрабатывается идемпотентно.

## Структура Java-проекта

- `src/main/java/ru/eapo/stub` — Java-код сервиса;
- `src/main/resources/openapi.json` — контракт административного API;
- `compose.json` — Docker Compose в JSON-формате;
- `src/test/java` — JUnit-тесты доменной логики.
