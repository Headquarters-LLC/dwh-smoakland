from __future__ import annotations
import time
import logging
from typing import Any, Dict, Optional
import requests

from . import config

log = logging.getLogger(__name__)


class QBOGatewayClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
    ):
        self.base_url = (base_url or config.get_base_url()).rstrip("/")
        self.api_key = api_key or config.get_api_key()
        self.timeout = timeout or config.get_timeout_seconds()
        self.retries = retries if retries is not None else config.get_retry_attempts()
        self.backoff = backoff if backoff is not None else config.get_retry_backoff()
        self.session = requests.Session()

    def _full_url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def post(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = self._full_url(path)
        hdrs = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
        }
        if headers:
            hdrs.update(headers)

        attempts = max(1, self.retries)
        last_exc: Exception | None = None
        for i in range(1, attempts + 1):
            try:
                resp = self.session.post(url, json=payload, headers=hdrs, params=params, timeout=self.timeout)
                if resp.status_code in {429} or resp.status_code >= 500:
                    log.warning(
                        "[qbo-gateway] transient status %s on attempt %s/%s",
                        resp.status_code,
                        i,
                        attempts,
                    )
                    resp.raise_for_status()
                resp.raise_for_status()
                return resp.json() if resp.content else {}
            except Exception as exc:  # pragma: no cover - exercised in integration tests
                last_exc = exc
                if i >= attempts:
                    break
                time.sleep(self.backoff * i)
        if last_exc:
            raise last_exc
        return {}


__all__ = ["QBOGatewayClient"]
