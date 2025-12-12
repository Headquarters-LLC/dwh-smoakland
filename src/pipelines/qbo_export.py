from __future__ import annotations
import os
from datetime import date
from typing import List, Optional, Sequence
import logging
from pathlib import Path

import pandas as pd
import requests

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
from src.notify.handlers import send_email
from src.warehouse.warehouse_duckdb import DuckDBWarehouse
from src.warehouse.warehouse_base import WarehouseBase

log = logging.getLogger(__name__)
SAMPLES_PATH = Path(__file__).resolve().parents[2] / "data_samples" / "qbo"
SKIPPED_COLS = [
    "bank_account",
    "subentity",
    "bank_account_cc",
    "date",
    "week_label",
    "description",
    "extended_description",
    "amount",
    "balance",
    "year",
    "payee_vendor",
    "cf_account",
    "dashboard_1",
    "budget_owner",
    "entity_qbo",
    "qbo_account",
    "qbo_sub_account",
    "week_num",
    "payee_vendor_rule_tag",
    "cf_account_rule_tag",
    "dashboard_1_rule_tag",
    "budget_owner_rule_tag",
    "entity_qbo_rule_tag",
    "qbo_account_rule_tag",
    "qbo_sub_account_rule_tag",
    "bank_cc_num",
    "txn_id",
    "realme_client_name",
    "drive_location",
    "drive_folder",
    "class",
    "skip_reason",
]


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


def _normalize_source(source: Optional[str]) -> str:
    if isinstance(source, str) and source.lower() in {"warehouse", "samples"}:
        return source.lower()
    return "warehouse"


def _load_samples_dataframe(base_path: Path | str | None = None) -> pd.DataFrame:
    base = Path(base_path) if base_path else SAMPLES_PATH
    if not base.exists():
        return pd.DataFrame()
    files = sorted(base.rglob("*.csv"))
    if not files:
        return pd.DataFrame()
    frames = []
    for fp in files:
        frames.append(pd.read_csv(fp))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return df

    if "bank_account_cc" not in df.columns:
        ba = df.get("bank_account", pd.Series([""] * len(df)))
        cc = df.get("bank_cc_num", pd.Series([""] * len(df)))
        df["bank_account_cc"] = (ba.fillna("").astype(str) + " " + cc.fillna("").astype(str)).str.strip()

    required_cols = [
        "bank_account",
        "bank_cc_num",
        "bank_account_cc",
        "payee_vendor",
        "description",
        "extended_description",
        "drive_location",
        "drive_folder",
        "amount",
        "date",
        "txn_id",
        "qbo_account",
        "qbo_sub_account",
        "entity_qbo",
        "class",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    return df


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


def _deposit_skip_reason(row: dict) -> str | None:
    amt = pd.to_numeric(row.get("amount"), errors="coerce")
    if pd.isna(amt) or amt <= 0:
        return "non_positive_amount"

    ba = _clean_str(row.get("bank_account"))
    cc = _clean_str(row.get("bank_cc_num"))
    deposit_to = _clean_str(row.get("bank_account_cc")) or f"{ba} {cc}".strip()
    if not deposit_to or _is_unknown(deposit_to) or _is_unknown(ba) or _is_unknown(cc):
        return "unknown_deposit_to_account"

    account_raw = _clean_str(row.get("qbo_account"))
    sub_account_raw = _clean_str(row.get("qbo_sub_account"))
    account = _combine_account(account_raw, sub_account_raw)
    if not account or _is_unknown(account) or _is_unknown(account_raw) or _is_unknown(sub_account_raw):
        return "unknown_account"

    entity_name = _clean_str(row.get("payee_vendor")) or None
    if _is_unknown(entity_name):
        return "unknown_entity"

    return None


def _expense_skip_reason(row: dict) -> str | None:
    amt = pd.to_numeric(row.get("amount"), errors="coerce")
    if pd.isna(amt) or amt >= 0:
        return "non_negative_amount"

    account_raw = _clean_str(row.get("qbo_account"))
    sub_account_raw = _clean_str(row.get("qbo_sub_account"))
    account = _combine_account(account_raw, sub_account_raw)
    if not account or _is_unknown(account) or _is_unknown(account_raw) or _is_unknown(sub_account_raw):
        return "unknown_account"

    bank_cc = _clean_str(row.get("bank_account_cc"))
    ba = _clean_str(row.get("bank_account"))
    cc = _clean_str(row.get("bank_cc_num"))
    bank = bank_cc or f"{ba} {cc}".strip()
    if not bank or _is_unknown(bank) or _is_unknown(ba) or _is_unknown(cc):
        return "unknown_bank_account"

    vendor = _clean_str(row.get("payee_vendor"))
    if not vendor or _is_unknown(vendor):
        return "unknown_vendor"

    return None


def _filter_skipped_for_report(df: pd.DataFrame, exclude_reasons: set[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    mask = ~df["skip_reason"].isin(exclude_reasons)
    return df.loc[mask].reset_index(drop=True)


def _trim_columns(df: pd.DataFrame, keep: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=keep)
    out = df.copy()
    for c in keep:
        if c not in out.columns:
            out[c] = ""
    return out[keep]


def _rows_by_txn(df: pd.DataFrame, txn_ids: List[str]) -> List[dict]:
    if df is None or df.empty or not txn_ids:
        return []
    # Normalize to strings so int txn_ids from warehouses match string ids on models.
    norm_ids = {_clean_str(tid) for tid in txn_ids if _clean_str(tid)}
    if not norm_ids:
        return []
    txn_series = df.get("txn_id")
    if txn_series is None:
        return []
    normalized = txn_series.apply(_clean_str)
    mask = normalized.isin(norm_ids)
    if not mask.any():
        return []
    rows = df.loc[mask].copy()
    rows["txn_id"] = normalized.loc[mask].values
    return rows.to_dict(orient="records")


def _append_skip_rows(skipped: pd.DataFrame, rows: List[dict], reason: str) -> pd.DataFrame:
    if rows is None or not rows:
        return skipped
    extras = []
    for r in rows:
        rd = dict(r)
        rd["skip_reason"] = reason
        extras.append(rd)
    if skipped is None or skipped.empty:
        return pd.DataFrame(extras)
    return pd.concat([skipped, pd.DataFrame(extras)], ignore_index=True)


def _merge_skipped(existing: pd.DataFrame, extra: pd.DataFrame) -> pd.DataFrame:
    if extra is None or extra.empty:
        return existing
    if existing is None or existing.empty:
        return extra
    return pd.concat([existing, extra], ignore_index=True)


def _idempotent_reuse_txns(responses: list[dict], items: Sequence) -> List[str]:
    """
    Gateway returns idempotent_reuse=True when it replays a prior request. That
    means we effectively skipped sending the row in this run, so surface those
    txn_ids for reporting.
    """
    if not responses or not items:
        return []
    txns: List[str] = []
    for item, resp in zip(items, responses):
        if isinstance(resp, dict) and resp.get("idempotent_reuse") and getattr(item, "txn_id", None):
            txns.append(item.txn_id)
    return txns


def _infer_week_num(df: pd.DataFrame, default: Optional[str] = None) -> str | None:
    if df is None or df.empty:
        return default
    wk = df.get("week_num")
    if wk is None:
        return default
    vals = wk.dropna()
    if vals.empty:
        return default
    try:
        return str(int(vals.iloc[0]))
    except Exception:
        return default


def _write_skipped_report(
    skipped: pd.DataFrame,
    week_start: date | str,
    week_end: date | str,
    kind: str,
    exclude_reasons: set[str],
) -> str | None:
    if skipped is None or skipped.empty:
        return None
    filtered = _filter_skipped_for_report(skipped, exclude_reasons)
    if filtered.empty:
        return None
    trimmed = _trim_columns(filtered, SKIPPED_COLS)
    report_dir = "/opt/airflow/logs/reports"
    os.makedirs(report_dir, exist_ok=True)
    skipped_path = os.path.join(report_dir, f"skipped_{kind}_{week_start}_{week_end}.csv")
    trimmed.to_csv(skipped_path, index=False)
    return skipped_path


def _desc(row: dict) -> str:
    base = _clean_str(row.get("description"))
    ext = _clean_str(row.get("extended_description"))
    if ext and ext not in base:
        return f"{base} - {ext}" if base else ext
    return base


def _private_note(row: dict) -> str:
    base = _clean_str(row.get("drive_location") or row.get("drive_folder") or "")
    txn_id = row.get("txn_id")

    if txn_id:
        if base:
            return f"{base}\nTransaction Id: {txn_id}"
        else:
            return f"Transaction Id: {txn_id}"

    return base


def load_categorized_transactions(
    start_date: date | str,
    end_date: date | str,
    *,
    warehouse: Optional[WarehouseBase] = None,
    source: str = "warehouse",
    samples_base: Path | str | None = None,
) -> pd.DataFrame:
    src = _normalize_source(source)
    if src == "samples":
        return _load_samples_dataframe(base_path=samples_base)
    wh = warehouse or _get_warehouse()
    start = pd.to_datetime(start_date, errors="coerce").date()
    end = pd.to_datetime(end_date, errors="coerce").date()
    return wh.fetch_categorized_between(start, end)


def build_deposits(df: pd.DataFrame) -> tuple[List[Deposit], pd.DataFrame]:
    if df is None or df.empty:
        return [], pd.DataFrame()

    mask = pd.to_numeric(df.get("amount"), errors="coerce") > 0
    working = df.loc[mask].copy()
    if working.empty:
        return [], pd.DataFrame()

    out: List[Deposit] = []
    skipped: list[dict] = []
    for _, row in working.iterrows():
        reason = _deposit_skip_reason(row)
        txn_id = _clean_str(row.get("txn_id"))
        if reason:
            log.warning(f"[qbo-export] skipping deposit txn_id={txn_id} reason={reason}")
            rd = row.to_dict()
            rd["skip_reason"] = reason
            skipped.append(rd)
            continue

        amt = pd.to_numeric(row.get("amount"), errors="coerce")
        ba = _clean_str(row.get("bank_account"))
        cc = _clean_str(row.get("bank_cc_num"))
        deposit_to = _clean_str(row.get("bank_account_cc")) or f"{ba} {cc}".strip()
        account_raw = _clean_str(row.get("qbo_account"))
        sub_account_raw = _clean_str(row.get("qbo_sub_account"))
        account = _combine_account(account_raw, sub_account_raw)
        entity_name = _clean_str(row.get("payee_vendor")) or None

        line = DepositLine(
            description=_desc(row),
            amount=float(abs(amt)),
            account_or_item=account,
            entity_name=entity_name,
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
    return out, pd.DataFrame(skipped)


def build_expenses(df: pd.DataFrame) -> tuple[List[Expense], pd.DataFrame]:
    if df is None or df.empty:
        return [], pd.DataFrame()

    mask = pd.to_numeric(df.get("amount"), errors="coerce") < 0
    working = df.loc[mask].copy()
    if working.empty:
        return [], pd.DataFrame()

    out: List[Expense] = []
    skipped: list[dict] = []
    for _, row in working.iterrows():
        reason = _expense_skip_reason(row)
        txn_id = _clean_str(row.get("txn_id"))
        if reason:
            log.warning(f"[qbo-export] skipping expense txn_id={txn_id} reason={reason}")
            rd = row.to_dict()
            rd["skip_reason"] = reason
            skipped.append(rd)
            continue

        amt = pd.to_numeric(row.get("amount"), errors="coerce")
        account_raw = _clean_str(row.get("qbo_account"))
        sub_account_raw = _clean_str(row.get("qbo_sub_account"))
        account = _combine_account(account_raw, sub_account_raw)

        line = ExpenseLine(
            expense_account=account,
            description=_desc(row),
            amount=float(abs(amt)),
            class_name=_clean_str(row.get("class")) or None,
        )
        note = _private_note(row)
        bank = _clean_str(row.get("bank_account_cc")) or f"{_clean_str(row.get('bank_account'))} {_clean_str(row.get('bank_cc_num'))}".strip()
        vendor = _clean_str(row.get("payee_vendor"))
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
    return out, pd.DataFrame(skipped)


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
    source: str = "warehouse",
    samples_base: Path | str | None = None,
) -> dict:
    env = (environment or qbo_config.get_default_environment()).lower()
    if auto_create is None:
        auto_create = env == "sandbox"

    df = load_categorized_transactions(week_start, week_end, warehouse=warehouse, source=source, samples_base=samples_base)
    deposits, skipped = build_deposits(df)
    week_num = _infer_week_num(df)
    log.info("[qbo-export] dry_run=%s", qbo_config.is_dry_run())
    if qbo_config.is_dry_run():
        log.info("[qbo-export] DRY RUN enabled -> skipping gateway calls for deposits")
        skipped_path = _write_skipped_report(skipped, week_start, week_end, "deposits", {"non_positive_amount"})
        return {"count": len(deposits), "responses": [], "skipped_path": skipped_path, "week_num": week_num}

    exp = exporter or QBOGatewayExporter()
    responses = []
    try:
        responses = _export_deposits(
            deposits,
            exp,
            client_id,
            environment=env,
            auto_create=auto_create,
            batch_size=batch_size,
        )
    except requests.HTTPError as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 409:
            txn_ids = [d.txn_id for d in deposits]
            dup_rows = _rows_by_txn(df, txn_ids)
            skipped = _append_skip_rows(skipped, dup_rows, reason="duplicate_transaction")
            log.warning("[qbo-export] duplicate deposit(s) detected -> marked as skipped duplicate_transaction")
        else:
            log.error("[qbo-export] gateway error status=%s -> marking deposits as skipped gateway_error", status, exc_info=True)
            skip_rows = _rows_by_txn(df, [d.txn_id for d in deposits])
            skipped = _append_skip_rows(skipped, skip_rows, reason="gateway_error")
            responses = []
    reused = _rows_by_txn(df, _idempotent_reuse_txns(responses, deposits))
    skipped = _append_skip_rows(skipped, reused, reason="idempotent_reuse")
    skipped_path = _write_skipped_report(skipped, week_start, week_end, "deposits", {"non_positive_amount"})
    return {"count": len(deposits), "responses": responses, "skipped_path": skipped_path, "week_num": week_num}


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
    source: str = "warehouse",
    samples_base: Path | str | None = None,
) -> dict:
    env = (environment or qbo_config.get_default_environment()).lower()
    if auto_create is None:
        auto_create = env == "sandbox"

    df = load_categorized_transactions(week_start, week_end, warehouse=warehouse, source=source, samples_base=samples_base)
    expenses, skipped = build_expenses(df)
    week_num = _infer_week_num(df)
    log.info("[qbo-export] dry_run=%s", qbo_config.is_dry_run())
    if qbo_config.is_dry_run():
        log.info("[qbo-export] DRY RUN enabled -> skipping gateway calls for expenses")
        skipped_path = _write_skipped_report(skipped, week_start, week_end, "expenses", {"non_negative_amount"})
        return {"count": len(expenses), "responses": [], "skipped_path": skipped_path, "week_num": week_num}

    exp = exporter or QBOGatewayExporter()
    responses = []
    try:
        responses = _export_expenses(
            expenses,
            exp,
            client_id,
            environment=env,
            auto_create=auto_create,
            batch_size=batch_size,
        )
    except requests.HTTPError as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 409:
            txn_ids = [e.txn_id for e in expenses]
            dup_rows = _rows_by_txn(df, txn_ids)
            skipped = _append_skip_rows(skipped, dup_rows, reason="duplicate_transaction")
            log.warning("[qbo-export] duplicate expense(s) detected -> marked as skipped duplicate_transaction")
        else:
            log.error("[qbo-export] gateway error status=%s -> marking expenses as skipped gateway_error", status, exc_info=True)
            skip_rows = _rows_by_txn(df, [e.txn_id for e in expenses])
            skipped = _append_skip_rows(skipped, skip_rows, reason="gateway_error")
            responses = []
    reused = _rows_by_txn(df, _idempotent_reuse_txns(responses, expenses))
    skipped = _append_skip_rows(skipped, reused, reason="idempotent_reuse")
    skipped_path = _write_skipped_report(skipped, week_start, week_end, "expenses", {"non_negative_amount"})
    return {"count": len(expenses), "responses": responses, "skipped_path": skipped_path, "week_num": week_num}


def _maybe_warn_mapping(realme: str, reason: str, count: int) -> None:
    log.warning("[qbo-export] skipping realme_client_name=%s reason=%s count=%s", realme, reason, count)


def _realme_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=object)
    if "realme_client_name" not in df.columns:
        return pd.Series([""] * len(df))
    return df["realme_client_name"].apply(_clean_str)


def export_deposits_multi(
    week_start: date | str,
    week_end: date | str,
    realme_to_client: dict[str, str],
    *,
    environment: Optional[str] = None,
    auto_create: Optional[bool] = None,
    warehouse: Optional[WarehouseBase] = None,
    exporter: Optional[QBOGatewayExporter] = None,
    batch_size: int = 50,
    source: str = "warehouse",
    samples_base: Path | str | None = None,
) -> dict:
    env = (environment or qbo_config.get_default_environment()).lower()
    if auto_create is None:
        auto_create = env == "sandbox"

    df = load_categorized_transactions(week_start, week_end, warehouse=warehouse, source=source, samples_base=samples_base)
    week_num = _infer_week_num(df)
    skipped: pd.DataFrame = pd.DataFrame()
    if df is None or df.empty:
        return {"total_realme": 0, "per_realme": {}, "skipped_path": None, "week_num": week_num}

    if qbo_config.is_dry_run():
        log.info("[qbo-export] DRY RUN enabled -> skipping gateway calls for expenses (multi)")
        skipped_path = _write_skipped_report(skipped, week_start, week_end, "expenses", {"non_negative_amount"})
        return {"total_realme": 0, "per_realme": {}, "skipped_path": skipped_path, "week_num": week_num}

    if qbo_config.is_dry_run():
        log.info("[qbo-export] DRY RUN enabled -> skipping gateway calls for deposits (multi)")
        skipped_path = _write_skipped_report(skipped, week_start, week_end, "deposits", {"non_positive_amount"})
        return {"total_realme": 0, "per_realme": {}, "skipped_path": skipped_path, "week_num": week_num}

    working = df.copy()
    working["_realme"] = _realme_series(working)
    missing_mask = working["_realme"] == ""
    if missing_mask.any():
        missing_rows = working.loc[missing_mask]
        skipped = _append_skip_rows(skipped, missing_rows.to_dict(orient="records"), "missing_realme_client_name")
        log.warning("[qbo-export] skipping rows with missing realme_client_name count=%s", len(missing_rows))
    grouped_df = working.loc[~missing_mask]

    exp = exporter or QBOGatewayExporter()
    per_realme: dict[str, dict] = {}

    for realme, group in grouped_df.groupby("_realme"):
        client_id = realme_to_client.get(realme)
        if not client_id:
            _maybe_warn_mapping(realme, "mapping_not_found", len(group))
            skipped = _append_skip_rows(skipped, group.to_dict(orient="records"), "mapping_not_found")
            continue

        deposits, skipped_group = build_deposits(group)
        skipped = _merge_skipped(skipped, skipped_group)
        responses: list[dict] = []
        try:
            responses = _export_deposits(
                deposits,
                exp,
                client_id,
                environment=env,
                auto_create=auto_create,
                batch_size=batch_size,
            )
        except requests.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status == 409:
                txn_ids = [d.txn_id for d in deposits]
                dup_rows = _rows_by_txn(group, txn_ids)
                skipped = _append_skip_rows(skipped, dup_rows, reason="duplicate_transaction")
                log.warning("[qbo-export] duplicate deposit(s) detected -> marked as skipped duplicate_transaction")
            else:
                log.error(
                    "[qbo-export] gateway error status=%s -> marking deposits as skipped gateway_error",
                    status,
                    exc_info=True,
                )
                skip_rows = _rows_by_txn(group, [d.txn_id for d in deposits])
                skipped = _append_skip_rows(skipped, skip_rows, reason="gateway_error")
                responses = []
        reused = _rows_by_txn(group, _idempotent_reuse_txns(responses, deposits))
        skipped = _append_skip_rows(skipped, reused, reason="idempotent_reuse")
        log.info(
            "[accounting] Exported %s deposits for realme=%s client_id=%s (env=%s, auto_create=%s)",
            len(deposits),
            realme,
            client_id,
            env,
            auto_create,
        )
        per_realme[realme] = {"client_id": client_id, "deposits": len(deposits), "responses": responses}

    skipped_path = _write_skipped_report(skipped, week_start, week_end, "deposits", {"non_positive_amount"})
    return {
        "total_realme": len(per_realme),
        "per_realme": per_realme,
        "skipped_path": skipped_path,
        "week_num": week_num,
    }


def export_expenses_multi(
    week_start: date | str,
    week_end: date | str,
    realme_to_client: dict[str, str],
    *,
    environment: Optional[str] = None,
    auto_create: Optional[bool] = None,
    warehouse: Optional[WarehouseBase] = None,
    exporter: Optional[QBOGatewayExporter] = None,
    batch_size: int = 50,
    source: str = "warehouse",
    samples_base: Path | str | None = None,
) -> dict:
    env = (environment or qbo_config.get_default_environment()).lower()
    if auto_create is None:
        auto_create = env == "sandbox"

    df = load_categorized_transactions(week_start, week_end, warehouse=warehouse, source=source, samples_base=samples_base)
    week_num = _infer_week_num(df)
    skipped: pd.DataFrame = pd.DataFrame()
    if df is None or df.empty:
        return {"total_realme": 0, "per_realme": {}, "skipped_path": None, "week_num": week_num}

    if qbo_config.is_dry_run():
        log.info("[qbo-export] DRY RUN enabled -> skipping gateway calls for expenses (multi)")
        skipped_path = _write_skipped_report(skipped, week_start, week_end, "expenses", {"non_negative_amount"})
        return {"total_realme": 0, "per_realme": {}, "skipped_path": skipped_path, "week_num": week_num}

    working = df.copy()
    working["_realme"] = _realme_series(working)
    missing_mask = working["_realme"] == ""
    if missing_mask.any():
        missing_rows = working.loc[missing_mask]
        skipped = _append_skip_rows(skipped, missing_rows.to_dict(orient="records"), "missing_realme_client_name")
        log.warning("[qbo-export] skipping rows with missing realme_client_name count=%s", len(missing_rows))
    grouped_df = working.loc[~missing_mask]

    exp = exporter or QBOGatewayExporter()
    per_realme: dict[str, dict] = {}

    for realme, group in grouped_df.groupby("_realme"):
        client_id = realme_to_client.get(realme)
        if not client_id:
            _maybe_warn_mapping(realme, "mapping_not_found", len(group))
            skipped = _append_skip_rows(skipped, group.to_dict(orient="records"), "mapping_not_found")
            continue

        expenses, skipped_group = build_expenses(group)
        skipped = _merge_skipped(skipped, skipped_group)
        responses: list[dict] = []
        try:
            responses = _export_expenses(
                expenses,
                exp,
                client_id,
                environment=env,
                auto_create=auto_create,
                batch_size=batch_size,
            )
        except requests.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status == 409:
                txn_ids = [e.txn_id for e in expenses]
                dup_rows = _rows_by_txn(group, txn_ids)
                skipped = _append_skip_rows(skipped, dup_rows, reason="duplicate_transaction")
                log.warning("[qbo-export] duplicate expense(s) detected -> marked as skipped duplicate_transaction")
            else:
                log.error(
                    "[qbo-export] gateway error status=%s -> marking expenses as skipped gateway_error",
                    status,
                    exc_info=True,
                )
                skip_rows = _rows_by_txn(group, [e.txn_id for e in expenses])
                skipped = _append_skip_rows(skipped, skip_rows, reason="gateway_error")
                responses = []
        reused = _rows_by_txn(group, _idempotent_reuse_txns(responses, expenses))
        skipped = _append_skip_rows(skipped, reused, reason="idempotent_reuse")
        log.info(
            "[accounting] Exported %s expenses for realme=%s client_id=%s (env=%s, auto_create=%s)",
            len(expenses),
            realme,
            client_id,
            env,
            auto_create,
        )
        per_realme[realme] = {"client_id": client_id, "expenses": len(expenses), "responses": responses}

    skipped_path = _write_skipped_report(skipped, week_start, week_end, "expenses", {"non_negative_amount"})
    return {
        "total_realme": len(per_realme),
        "per_realme": per_realme,
        "skipped_path": skipped_path,
        "week_num": week_num,
    }


__all__ = [
    "load_categorized_transactions",
    "build_deposits",
    "build_expenses",
    "export_deposits",
    "export_expenses",
    "export_deposits_multi",
    "export_expenses_multi",
]
