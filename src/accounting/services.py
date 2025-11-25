from __future__ import annotations
from typing import Sequence, Callable
import logging
from .models import Deposit, Expense
from .ports import IAccountingExporter

log = logging.getLogger(__name__)


def _chunk(seq: Sequence, size: int) -> list[list]:
    return [list(seq[i:i + size]) for i in range(0, len(seq), size)]


def export_deposits(
    deposits: Sequence[Deposit],
    exporter: IAccountingExporter,
    client_id: str,
    *,
    environment: str = "sandbox",
    auto_create: bool = False,
    batch_size: int = 50,
) -> list[dict]:
    """
    Chunk deposits and delegate to the exporter.
    Returns a flattened list of responses.
    """
    if not deposits:
        log.info("[accounting] No deposits to export")
        return []

    results: list[dict] = []
    for chunk in _chunk(deposits, batch_size):
        keys = [d.idempotency_fingerprint(client_id) for d in chunk]
        try:
            resp = exporter.send_deposits(
                client_id,
                chunk,
                environment=environment,
                auto_create=auto_create,
                idempotency_keys=keys,
            ) or []
            results.extend(resp)
            log.info(
                "[accounting] Exported %s deposits (env=%s, auto_create=%s)",
                len(chunk),
                environment,
                auto_create,
            )
        except Exception:
            log.exception(
                "[accounting] Deposit export failed for batch size %s (env=%s)",
                len(chunk),
                environment,
            )
            raise
    return results


def export_expenses(
    expenses: Sequence[Expense],
    exporter: IAccountingExporter,
    client_id: str,
    *,
    environment: str = "sandbox",
    auto_create: bool = False,
    batch_size: int = 50,
) -> list[dict]:
    """
    Chunk expenses and delegate to the exporter.
    Returns a flattened list of responses.
    """
    if not expenses:
        log.info("[accounting] No expenses to export")
        return []

    results: list[dict] = []
    for chunk in _chunk(expenses, batch_size):
        keys = [e.idempotency_fingerprint(client_id) for e in chunk]
        try:
            resp = exporter.send_expenses(
                client_id,
                chunk,
                environment=environment,
                auto_create=auto_create,
                idempotency_keys=keys,
            ) or []
            results.extend(resp)
            log.info(
                "[accounting] Exported %s expenses (env=%s, auto_create=%s)",
                len(chunk),
                environment,
                auto_create,
            )
        except Exception:
            log.exception(
                "[accounting] Expense export failed for batch size %s (env=%s)",
                len(chunk),
                environment,
            )
            raise
    return results


__all__ = ["export_deposits", "export_expenses"]
