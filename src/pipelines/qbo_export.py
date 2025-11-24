from __future__ import annotations
import os
from datetime import date
from typing import List, Optional
import logging
import pandas as pd

from src.accounting.models import (
    Deposit,
    DepositLine,
    Expense,
    ExpenseLine,
    _combine_account,
)
from src.accounting.services import export_deposits as _export_deposits, export_expenses as _export_expenses
from src.integrations.qbo_gateway.exporter import QBOGatewayExporter
from src.integrations.qbo_gateway import config as qbo_config
from src.warehouse.warehouse_duckdb import DuckDBWarehouse
from src.warehouse.warehouse_base import WarehouseBase

log = logging.getLogger(__name__)


def _get_warehouse(db_path: Optional[str] = None) -> WarehouseBase:
    db = db_path or os.getenv("DUCKDB_PATH", "/opt/airflow/logs/local.duckdb")
    return DuckDBWarehouse(db_path=db)


def _clean_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    text = str(val).strip()
    return "" if text.lower() == "nan" else text


def _is_unknown(val) -> bool:
    """
    Treat common textual variants as 'unknown' so we can skip bad rows upstream.
    """
    norm = _clean_str(val).lower()
    if not norm:
        return False
    return norm in {
        "unknown",
        "unknow",  # typo seen in the wild
        "desconocido",
        "desconocida",
    }


def _normalize_date(val) -> date:
    ts = pd.to_datetime(val, errors="coerce")
    if pd.isna(ts):
        return pd.Timestamp.utcnow().date()
    return ts.date()


def _desc(row: dict) -> str:
    base = _clean_str(row.get("description"))
    ext = _clean_str(row.get("extended_description"))
    if ext and ext not in base:
        return f"{base} - {ext}" if base else ext
    return base


def _private_note(row: dict) -> str:
    # Per Annex: only drive_location is used as memo; extended desc stays in line description.
    return _clean_str(row.get("drive_location") or row.get("drive_folder"))


def load_categorized_transactions(
    start_date: date | str,
    end_date: date | str,
    warehouse: Optional[WarehouseBase] = None,
) -> pd.DataFrame:
    wh = warehouse or _get_warehouse()
    start = pd.to_datetime(start_date, errors="coerce").date()
    end = pd.to_datetime(end_date, errors="coerce").date()
    return wh.fetch_categorized_between(start, end)


def build_deposits(df: pd.DataFrame) -> List[Deposit]:
    if df is None or df.empty:
        return []

    out: List[Deposit] = []
    for _, row in df.iterrows():
        amt = pd.to_numeric(row.get("amount"), errors="coerce")
        if pd.isna(amt) or amt <= 0:
            continue
        deposit_to = _clean_str(row.get("bank_account_cc"))
        if not deposit_to:
            ba = _clean_str(row.get("bank_account"))
            cc = _clean_str(row.get("bank_cc_num"))
            deposit_to = f"{ba} {cc}".strip()
        account = _combine_account(_clean_str(row.get("qbo_account")), _clean_str(row.get("qbo_sub_account")))
        txn_id = _clean_str(row.get("txn_id"))

        if not deposit_to or _is_unknown(deposit_to):
            log.warning("[qbo-export] skipping deposit txn_id=%s field=deposit_to_account reason=unknown_account", txn_id)
            continue
        if not account or _is_unknown(account):
            # Without an account, QBO cannot categorize the line; skip for safety.
            log.warning("[qbo-export] skipping deposit txn_id=%s field=account reason=unknown_account", txn_id)
            continue
        entity_name = _clean_str(row.get("payee_vendor")) or None
        if _is_unknown(entity_name):
            log.warning("[qbo-export] skipping deposit txn_id=%s field=entity_name reason=unknown_entity", txn_id)
            continue

        line = DepositLine(
            description=_desc(row),
            amount=float(abs(amt)),
            account_or_item=account,
            entity_name=entity_name,
            # Temporary fallback until full resolver lands (Increment 3).
            entity_type=_clean_str(row.get("entity_type")) or "Customer",
        )
        note = _private_note(row)
        d = Deposit(
            txn_id=txn_id,
            date=_normalize_date(row.get("date")),
            deposit_to_account=deposit_to,
            doc_number=txn_id,
            lines=[line],
            private_note=note or None,
        )
        out.append(d)
    return out


def build_expenses(df: pd.DataFrame) -> List[Expense]:
    if df is None or df.empty:
        return []

    out: List[Expense] = []
    for _, row in df.iterrows():
        amt = pd.to_numeric(row.get("amount"), errors="coerce")
        if pd.isna(amt) or amt >= 0:
            continue
        account = _combine_account(_clean_str(row.get("qbo_account")), _clean_str(row.get("qbo_sub_account")))
        txn_id = _clean_str(row.get("txn_id"))
        if not account or _is_unknown(account):
            log.warning("[qbo-export] skipping expense txn_id=%s field=expense_account reason=unknown_account", txn_id)
            continue

        bank = _clean_str(row.get("bank_account_cc"))
        if not bank:
            ba = _clean_str(row.get("bank_account"))
            cc = _clean_str(row.get("bank_cc_num"))
            bank = f"{ba} {cc}".strip()
        if not bank or _is_unknown(bank):
            log.warning("[qbo-export] skipping expense txn_id=%s field=bank_account reason=unknown_bank_account", txn_id)
            continue

        vendor = _clean_str(row.get("payee_vendor"))
        if not vendor or _is_unknown(vendor):
            log.warning("[qbo-export] skipping expense txn_id=%s field=vendor reason=unknown_vendor", txn_id)
            continue

        line = ExpenseLine(
            expense_account=account,
            description=_desc(row),
            amount=float(abs(amt)),
            class_name=_clean_str(row.get("class")) or None,
        )
        note = _private_note(row)
        e = Expense(
            txn_id=txn_id,
            date=_normalize_date(row.get("date")),
            vendor=vendor,
            bank_account=bank,
            doc_number=txn_id,
            lines=[line],
            private_note=note or None,
        )
        out.append(e)
    return out


def export_deposits(
    week_start: date | str,
    week_end: date | str,
    client_id: str,
    *,
    environment: Optional[str] = None,
    auto_create: Optional[bool] = None,
    warehouse: Optional[WarehouseBase] = None,
    exporter: Optional[QBOGatewayExporter] = None,
    batch_size: int = 50,
) -> dict:
    env = (environment or qbo_config.get_default_environment()).lower()
    if auto_create is None:
        auto_create = env == "sandbox"

    df = load_categorized_transactions(week_start, week_end, warehouse)
    deposits = build_deposits(df)
    exp = exporter or QBOGatewayExporter()
    responses = _export_deposits(
        deposits,
        exp,
        client_id,
        environment=env,
        auto_create=auto_create,
        batch_size=batch_size,
    )
    return {"count": len(deposits), "responses": responses}


def export_expenses(
    week_start: date | str,
    week_end: date | str,
    client_id: str,
    *,
    environment: Optional[str] = None,
    auto_create: Optional[bool] = None,
    warehouse: Optional[WarehouseBase] = None,
    exporter: Optional[QBOGatewayExporter] = None,
    batch_size: int = 50,
) -> dict:
    env = (environment or qbo_config.get_default_environment()).lower()
    if auto_create is None:
        auto_create = env == "sandbox"

    df = load_categorized_transactions(week_start, week_end, warehouse)
    expenses = build_expenses(df)
    exp = exporter or QBOGatewayExporter()
    responses = _export_expenses(
        expenses,
        exp,
        client_id,
        environment=env,
        auto_create=auto_create,
        batch_size=batch_size,
    )
    return {"count": len(expenses), "responses": responses}


__all__ = [
    "load_categorized_transactions",
    "build_deposits",
    "build_expenses",
    "export_deposits",
    "export_expenses",
]
