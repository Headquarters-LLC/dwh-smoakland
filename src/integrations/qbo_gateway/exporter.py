from __future__ import annotations
import logging
from typing import Sequence, Optional, Dict, Any

from src.accounting.models import Deposit, Expense
from src.accounting.ports import IAccountingExporter
from .client import QBOGatewayClient
from .mappers import deposit_to_payload, expense_to_payload
from . import config

log = logging.getLogger(__name__)


class QBOGatewayExporter(IAccountingExporter):
    def __init__(self, client: Optional[QBOGatewayClient] = None):
        self.client = client or QBOGatewayClient()

    def _default_params(self, environment: str, auto_create: bool) -> Dict[str, Any]:
        params: Dict[str, Any] = {"environment": environment or config.get_default_environment()}
        if auto_create:
            params["auto_create"] = "true"
        return params

    def send_deposits(
        self,
        client_id: str,
        deposits: Sequence[Deposit],
        *,
        environment: str = "sandbox",
        auto_create: bool = False,
        idempotency_keys: Optional[Sequence[str]] = None,
    ) -> list[dict]:
        results: list[dict] = []
        env = (environment or config.get_default_environment()).lower()
        for idx, deposit in enumerate(deposits):
            idemp = None
            if idempotency_keys and idx < len(idempotency_keys):
                idemp = idempotency_keys[idx]
            payload = deposit_to_payload(deposit)
            headers = {"Idempotency-Key": idemp or deposit.txn_id}
            path = f"/qbo/{client_id}/deposits"
            resp = self.client.post(
                path,
                payload,
                headers=headers,
                params=self._default_params(env, auto_create),
            )
            results.append(resp)
            log.info(
                "[qbo-gateway] deposit sent txn_id=%s amount=%.2f env=%s",
                deposit.txn_id,
                deposit.total_amount,
                env,
            )
        return results

    def send_expenses(
        self,
        client_id: str,
        expenses: Sequence[Expense],
        *,
        environment: str = "sandbox",
        auto_create: bool = False,
        idempotency_keys: Optional[Sequence[str]] = None,
    ) -> list[dict]:
        results: list[dict] = []
        env = (environment or config.get_default_environment()).lower()
        for idx, expense in enumerate(expenses):
            idemp = None
            if idempotency_keys and idx < len(idempotency_keys):
                idemp = idempotency_keys[idx]
            payload = expense_to_payload(expense)
            headers = {"Idempotency-Key": idemp or expense.txn_id}
            path = f"/qbo/{client_id}/expenses"
            resp = self.client.post(
                path,
                payload,
                headers=headers,
                params=self._default_params(env, auto_create),
            )
            results.append(resp)
            log.info(
                "[qbo-gateway] expense sent txn_id=%s amount=%.2f env=%s",
                expense.txn_id,
                expense.total_amount,
                env,
            )
        return results


__all__ = ["QBOGatewayExporter"]
