from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import uuid

REQUEST_TYPE = "APPLICATION_STATUS_CHANGE_REQUESTED"
RESULT_TYPE = "APPLICATION_STATUS_CHANGE_RESULT"


@dataclass(frozen=True)
class Settings:
    processing_mode: str = "STATUS_TRANSITION"
    result_publish_delay_ms: int = 0
    error_simulation_enabled: bool = False
    error_code: str = "INTERNAL_ERROR"
    error_message: str = "Test processing error"
    retryable: bool = False


def decode_message_key(raw: bytes | None) -> str:
    """Decode both plain Kafka string keys and JSON keys produced by REST Proxy."""
    if not raw:
        return ""
    text = raw.decode("utf-8")
    try:
        decoded = json.loads(text)
        return decoded if isinstance(decoded, str) else text
    except json.JSONDecodeError:
        return text


def fingerprint(event: dict) -> str:
    body = {"applicationId": event.get("applicationId"), "currentStatus": event.get("currentStatus")}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def validate_request(event: dict, message_key: str) -> None:
    required = ("eventId", "requestId", "correlationId", "applicationId", "currentStatus")
    missing = [name for name in required if not event.get(name)]
    if missing:
        raise ValueError("INVALID_MESSAGE:" + ",".join(missing))
    if event.get("eventType") != REQUEST_TYPE or event.get("producer") != "eapo-cab":
        raise ValueError("UNSUPPORTED_EVENT")
    if event.get("eventVersion") != 1:
        raise ValueError("UNSUPPORTED_EVENT_VERSION")
    if event["applicationId"] != message_key:
        raise ValueError("APPLICATION_KEY_MISMATCH")


def build_result(event: dict, next_status: str | None, error: tuple[str, str, bool] | None = None) -> dict:
    result = {
        "eventId": str(uuid.uuid4()),
        "eventType": RESULT_TYPE,
        "eventVersion": 1,
        "occurredAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "requestId": event.get("requestId"),
        "correlationId": event.get("correlationId") or event.get("requestId"),
        "causationId": event.get("eventId"),
        "producer": "status-stub",
        "applicationId": event.get("applicationId"),
        "previousStatus": event.get("currentStatus"),
        "status": next_status,
        "result": "ERROR" if error else "SUCCESS",
    }
    if error:
        result.update(errorCode=error[0], message=error[1], retryable=error[2])
    return result


def choose_status(current: str, settings: Settings, transitions: dict[str, str]) -> str:
    if settings.processing_mode == "ALWAYS_COMPLETED":
        return "COMPLETED"
    if settings.processing_mode == "KEEP_CURRENT_STATUS":
        return current
    if current not in transitions:
        raise LookupError("UNKNOWN_STATUS")
    return transitions[current]
