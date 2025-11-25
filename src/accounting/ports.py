from __future__ import annotations
from typing import Protocol, Sequence, Optional
from .models import Deposit, Expense


class IAccountingExporter(Protocol):
    def send_deposits(
        self,
        client_id: str,
        deposits: Sequence[Deposit],
        *,
        environment: str = "sandbox",
        auto_create: bool = False,
        idempotency_keys: Optional[Sequence[str]] = None,
    ) -> list[dict]:
        ...

    def send_expenses(
        self,
        client_id: str,
        expenses: Sequence[Expense],
        *,
        environment: str = "sandbox",
        auto_create: bool = False,
        idempotency_keys: Optional[Sequence[str]] = None,
    ) -> list[dict]:
        ...


__all__ = ["IAccountingExporter"]
