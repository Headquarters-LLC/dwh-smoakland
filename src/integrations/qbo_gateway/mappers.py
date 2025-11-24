from __future__ import annotations
from typing import Dict, Any
from src.accounting.models import Deposit, Expense, _combine_account


def deposit_to_payload(deposit: Deposit) -> Dict[str, Any]:
    lines = []
    for line in deposit.lines:
        payload_line = {
            "description": line.description,
            "amount": float(line.amount),
            "account_or_item": line.account_or_item,
        }
        if line.entity_name:
            payload_line["entity_name"] = line.entity_name
        if line.entity_type:
            payload_line["entity_type"] = line.entity_type
        lines.append(payload_line)

    return {
        "deposit_to_account": deposit.deposit_to_account,
        "date": deposit.date.isoformat(),
        "doc_number": deposit.doc_number,
        "lines": lines,
        "private_note": deposit.private_note or "",
    }


def expense_to_payload(expense: Expense) -> Dict[str, Any]:
    lines = []
    for line in expense.lines:
        payload_line = {
            "expense_account": line.expense_account,
            "description": line.description,
            "amount": float(line.amount),
        }
        if line.class_name:
            payload_line["class"] = line.class_name
        lines.append(payload_line)
    return {
        "vendor": expense.vendor,
        "bank_account": expense.bank_account,
        "date": expense.date.isoformat(),
        "doc_number": expense.doc_number,
        "lines": lines,
        "private_note": expense.private_note or "",
    }


__all__ = ["deposit_to_payload", "expense_to_payload", "_combine_account"]
