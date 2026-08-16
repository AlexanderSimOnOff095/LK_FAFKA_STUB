OPENAPI = {
    "openapi": "3.0.3",
    "info": {
        "title": "LK Kafka STUB Admin API",
        "version": "1.0.0",
        "description": "Административный API сервиса-заглушки. Бизнес-обмен выполняется через Kafka topic applications.status.",
    },
    "servers": [{"url": "http://localhost:8090"}],
    "tags": [{"name": "Health"}, {"name": "Settings"}, {"name": "Transitions"}],
    "paths": {
        "/api/v1/admin/health": {"get": {"tags": ["Health"], "summary": "Проверка STUB, Kafka и PostgreSQL", "responses": {"200": {"description": "Сервис доступен"}, "503": {"description": "Компонент недоступен"}}}},
        "/api/v1/admin/settings": {"get": {"tags": ["Settings"], "summary": "Получить настройки", "responses": {"200": {"description": "Текущая конфигурация"}}}},
        "/api/v1/admin/settings/processing-mode": {"put": {"tags": ["Settings"], "summary": "Изменить режим обработки", "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ProcessingModeRequest"}}}}, "responses": {"200": {"description": "Режим изменен"}}}},
        "/api/v1/admin/settings/response-delay": {"put": {"tags": ["Settings"], "summary": "Настроить задержку Kafka RESULT", "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/DelayRequest"}}}}, "responses": {"200": {"description": "Задержка изменена"}}}},
        "/api/v1/admin/error-simulation": {"put": {"tags": ["Settings"], "summary": "Настроить имитацию ошибки", "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ErrorSimulationRequest"}}}}, "responses": {"200": {"description": "Настройка изменена"}}}},
        "/api/v1/admin/settings/reset": {"post": {"tags": ["Settings"], "summary": "Сбросить настройки", "responses": {"200": {"description": "Настройки восстановлены"}}}},
        "/api/v1/admin/status-transitions": {"get": {"tags": ["Transitions"], "summary": "Получить переходы статусов", "responses": {"200": {"description": "Список переходов"}}}},
        "/api/v1/admin/status-transitions/{currentStatus}": {
            "parameters": [{"name": "currentStatus", "in": "path", "required": True, "schema": {"type": "string"}}],
            "put": {"tags": ["Transitions"], "summary": "Создать или изменить переход", "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/TransitionRequest"}}}}, "responses": {"200": {"description": "Переход сохранен"}}},
            "delete": {"tags": ["Transitions"], "summary": "Удалить переход", "responses": {"204": {"description": "Переход удален"}}},
        },
    },
    "components": {"schemas": {
        "ProcessingModeRequest": {"type": "object", "required": ["processingMode"], "properties": {"processingMode": {"type": "string", "enum": ["STATUS_TRANSITION", "ALWAYS_COMPLETED", "KEEP_CURRENT_STATUS"]}}},
        "DelayRequest": {"type": "object", "required": ["resultPublishDelayMs"], "properties": {"resultPublishDelayMs": {"type": "integer", "minimum": 0, "maximum": 30000}}},
        "TransitionRequest": {"type": "object", "required": ["nextStatus"], "properties": {"nextStatus": {"type": "string"}}},
        "ErrorSimulationRequest": {"type": "object", "required": ["enabled"], "properties": {"enabled": {"type": "boolean"}, "errorCode": {"type": "string"}, "message": {"type": "string"}, "retryable": {"type": "boolean"}}},
    }},
}

SWAGGER_HTML = """<!doctype html>
<html lang=\"ru\"><head><meta charset=\"utf-8\"><title>LK Kafka STUB API</title>
<link rel=\"stylesheet\" href=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui.css\"></head>
<body><div id=\"swagger-ui\"></div>
<script src=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>
<script>SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui',deepLinking:true,tryItOutEnabled:true});</script>
</body></html>"""
