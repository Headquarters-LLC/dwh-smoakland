from __future__ import annotations
from datetime import date
from typing import List, Optional
import hashlib
from pydantic import BaseModel, Field


def _combine_account(account: str | None, sub_account: str | None) -> str:
    """
    Combine parent/sub account names using QuickBooks' colon notation.
    Empty components are ignored gracefully.
    """
    parts = [p for p in [account or "", sub_account or ""] if p]
    return ":".join(parts) if parts else ""


class DepositLine(BaseModel):
    description: str
    amount: float
    account_or_item: Optional[str] = None
    entity_name: Optional[str] = None
    entity_type: Optional[str] = None


class Deposit(BaseModel):
    txn_id: str
    date: date
    deposit_to_account: str
    doc_number: str = Field(alias="doc_number")
    lines: List[DepositLine]
    private_note: Optional[str] = None

    model_config = {"populate_by_name": True}

    @property
    def total_amount(self) -> float:
        return float(sum(line.amount for line in self.lines))

    def idempotency_fingerprint(self, realm_id: str) -> str:
        """
        For gateway v2, we now rely on txn_id directly as idempotency key.
        """
        return self.txn_id


class ExpenseLine(BaseModel):
    expense_account: str
    description: str
    amount: float
    class_name: Optional[str] = None


class Expense(BaseModel):
    txn_id: str
    date: date
    vendor: str
    bank_account: str
    doc_number: str = Field(alias="doc_number")
    lines: List[ExpenseLine]
    private_note: Optional[str] = None

    model_config = {"populate_by_name": True}

    @property
    def total_amount(self) -> float:
        return float(sum(line.amount for line in self.lines))

    def idempotency_fingerprint(self, realm_id: str) -> str:
        """
        For gateway v2, we now rely on txn_id directly as idempotency key.
        """
        return self.txn_id


__all__ = [
    "Deposit",
    "DepositLine",
    "Expense",
    "ExpenseLine",
    "_combine_account",
]
