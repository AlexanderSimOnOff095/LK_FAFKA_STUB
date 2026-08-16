# LK_FAFKA_STUB

Проект `STUB` для тестирования асинхронной смены статусов заявок EAPO-Cab через один Kafka-топик.

## Состав

- `applications.status` — единый топик REQUEST/RESULT;
- `stub-status-change-v1` — consumer group сервиса;
- `eapo-cab-status-result-v1` — consumer group Личного кабинета;
- PostgreSQL БД `lk_fafka_stub_kafka`;
- административный REST API на `http://localhost:8080`;
- Kafka UI на `http://localhost:8081`;
- Kafka REST Proxy на `http://localhost:8082` для Postman;
- две Postman-коллекции в `postman/`.

## Запуск

1. Установить и запустить Docker Desktop.
2. Скопировать `.env.example` в `.env`.
3. Выполнить:

```powershell
docker compose up -d --build
```

Топик и БД создаются автоматически. Проверка:

```powershell
docker compose ps
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic applications.status
Invoke-RestMethod http://localhost:8080/api/v1/admin/health
```

## Postman

Импортировать окружение `postman/LK_FAFKA_STUB.local.postman_environment.json` и коллекции:

- `postman/STUB_Admin.postman_collection.json` — настройка STUB;
- `postman/EAPO_Cab_Kafka_Status.postman_collection.json` — отправка REQUEST и чтение сообщений через Kafka REST Proxy.

Postman не использует бинарный Kafka-протокол напрямую. Для тестов коллекция обращается к REST Proxy на порту `8082`; Kafka UI на `8081` используется для просмотра топика и сообщений.

## DBeaver

Создать PostgreSQL connection:

- Host: `localhost`
- Port: `5432`
- Database: `lk_fafka_stub_kafka`
- User: `stub_kafka`
- Password: значение `POSTGRES_PASSWORD` из `.env`

DDL: `db/init/001_schema.sql`. Все имена баз данных проекта имеют суффикс `_kafka`.

## Тесты

Автономные unit-тесты не требуют Kafka/PostgreSQL:

```powershell
python -m unittest discover -s tests -v
```

Полная проверка после запуска Docker:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1
```

## Контракт

STUB читает только `APPLICATION_STATUS_CHANGE_REQUESTED` от `producer=eapo-cab`. Результат публикуется как `APPLICATION_STATUS_CHANGE_RESULT` от `producer=status-stub` с тем же Kafka key (`applicationId`). Offset подтверждается после публикации результата; повторный `requestId` обрабатывается идемпотентно.
