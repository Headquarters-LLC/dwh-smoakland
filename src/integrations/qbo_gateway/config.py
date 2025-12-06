from __future__ import annotations
import json
import logging
import os
from typing import Optional

try:
    from airflow.models import Variable
except Exception:  # pragma: no cover - optional during tests
    Variable = None  # type: ignore

log = logging.getLogger(__name__)


def _get_var(name: str, default: Optional[str] = None) -> str:
    if Variable is None:
        return os.getenv(name, default or "")
    try:
        return Variable.get(name, default_var=os.getenv(f"AIRFLOW_VAR_{name}", default))
    except Exception:
        return os.getenv(f"AIRFLOW_VAR_{name}", default or "")


def get_base_url() -> str:
    return _get_var("QBO_GATEWAY_BASE_URL", "http://localhost:8000")


def get_api_key() -> str:
    return _get_var("QBO_GATEWAY_API_KEY", "")


def get_default_environment() -> str:
    env = _get_var("QBO_ENVIRONMENT", "sandbox")
    return (env or "sandbox").lower()


def get_timeout_seconds() -> int:
    raw = _get_var("QBO_GATEWAY_TIMEOUT", "15")
    try:
        return int(raw)
    except Exception:
        return 15


def get_retry_attempts() -> int:
    raw = _get_var("QBO_GATEWAY_RETRY_ATTEMPTS", "3")
    try:
        return int(raw)
    except Exception:
        return 3


def get_retry_backoff() -> float:
    raw = _get_var("QBO_GATEWAY_RETRY_BACKOFF", "1.5")
    try:
        return float(raw)
    except Exception:
        return 1.5


def get_realme_clients_map() -> dict[str, str]:
    """
    Returns the mapping from realme_client_name to QBO Gateway client_id.
    Falls back to an empty dict when the variable is not configured or invalid so
    callers can choose whether to fail or continue in single-client mode.
    """
    raw = _get_var("QBO_REALME_CLIENTS", "")
    if not raw:
        log.warning("[qbo-export] QBO_REALME_CLIENTS not configured; defaulting to single-client mode")
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        log.error("[qbo-export] invalid JSON in QBO_REALME_CLIENTS; defaulting to single-client mode")
        return {}
    if not isinstance(data, dict):
        log.error("[qbo-export] QBO_REALME_CLIENTS must be a JSON object mapping realme_client_name to client_id")
        return {}
    # Normalize keys/values to strings for consistency.
    return {str(k): str(v) for k, v in data.items() if k is not None and v is not None}


__all__ = [
    "get_api_key",
    "get_base_url",
    "get_default_environment",
    "get_timeout_seconds",
    "get_retry_attempts",
    "get_retry_backoff",
    "get_realme_clients_map",
]
