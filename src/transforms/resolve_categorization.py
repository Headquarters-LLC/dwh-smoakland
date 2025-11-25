#resolve_categorization.py
from __future__ import annotations
from typing import Callable, Dict, List, Tuple
import pandas as pd

# Centralized mappings for gold.categorized_bank_cc
BANK_CC_NUM_TO_ACCOUNT_CC: Dict[str, str] = {
    "2035": "NBCU 2035",
    "71000": "CC 71000",
    "6852": "KP 6852",
    "8452": "EWB 8452",
    "9551": "CC 9551",
    "SWD": "NBCU SWD",
    "3439": "EWB 3439",
    "7639": "CC 7639",
    "8267": "CC 8267",
    "7403": "Dama 7403",
}

REALME_CLIENT_NAME_MAP: Dict[str, str] = {
    "SWD": "DeliverMD",
    "SSC": "Sublime Machining",
    "HAH 7 CA": "HAH 7 LLC",
    "NY": "TPH786",
    "SOCAL": "TPH786",
    "TPH": "TPH786",
}

# Import each rulebook's row-level API (infer)
from rulebook.payee_vendor import infer as infer_payee_vendor, postprocess as postprocess_payee_vendor
from rulebook.cf_account import infer as infer_cf_account
from rulebook.dashboard_1 import infer as infer_dashboard_1
from rulebook.budget_owner import infer as infer_budget_owner
from rulebook.entity_qbo import infer as infer_entity_qbo
from rulebook.qbo_account import infer as infer_qbo_account
from rulebook.qbo_sub_account import infer as infer_qbo_sub_account

# ---------------------------------------------------------------------------
# Configuration: which columns we resolve and with which rulebook.
# Order matters: they're applied sequentially and can build on prior results.
# Each infer_fn must return: (value: str, rule_tag: str)
#   value    -> inferred value or "UNKNOWN"/"" depending on rulebook's contract
#   rule_tag -> "rulebook@version#<rule_id>" or "" if unknown
# ---------------------------------------------------------------------------
RESOLVERS: List[Tuple[str, Callable[[dict], Tuple[str, str]]]] = [
    ("payee_vendor",     infer_payee_vendor),
    ("cf_account",       infer_cf_account),
    ("dashboard_1",      infer_dashboard_1),
    ("budget_owner",     infer_budget_owner),
    ("entity_qbo",       infer_entity_qbo),
    ("qbo_account",      infer_qbo_account),
    ("qbo_sub_account",  infer_qbo_sub_account),
]

# Final schema for gold.categorized_bank_cc
CATEG_COLS: List[str] = [
    # 1) Core / básicos
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

    # 2) Valores categorizados (uno por rulebook)
    "payee_vendor",
    "cf_account",
    "dashboard_1",
    "budget_owner",
    "entity_qbo",
    "qbo_account",
    "qbo_sub_account",

    # 3) Week info lógico
    "week_num",

    # 4) Metadatos de rulebooks (tag / confidence / source)
    "payee_vendor_rule_tag",
    "payee_vendor_confidence",
    "payee_vendor_source",

    "cf_account_rule_tag",
    "cf_account_confidence",
    "cf_account_source",

    "dashboard_1_rule_tag",
    "dashboard_1_confidence",
    "dashboard_1_source",

    "budget_owner_rule_tag",
    "budget_owner_confidence",
    "budget_owner_source",

    "entity_qbo_rule_tag",
    "entity_qbo_confidence",
    "entity_qbo_source",

    "qbo_account_rule_tag",
    "qbo_account_confidence",
    "qbo_account_source",

    "qbo_sub_account_rule_tag",
    "qbo_sub_account_confidence",
    "qbo_sub_account_source",

    # 5) Campos “identificadores” / auxiliares al final
    "bank_cc_num",
    "txn_id",
    "realme_client_name",
]

def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Add missing columns with NA to guarantee downstream schema."""
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out

def _normalize_description(series: pd.Series) -> pd.Series:
    """
    Trim and collapse repeated whitespace for descriptions.
    """
    if series is None:
        return pd.Series(dtype="object")
    if series.empty:
        return series

    out = series.copy()
    mask = out.notna()
    out.loc[mask] = (
        out.loc[mask]
        .astype(str)
        .str.strip()
        .str.replace(r"\s{2,}", " ", regex=True)
    )
    return out

def _normalized_cc_key(val) -> str | None:
    if pd.isna(val):
        return None
    v = val
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    key = str(v).strip()
    if key.endswith(".0"):
        key = key[:-2]
    key = key.upper()
    return key or None

def _map_bank_account_cc(df: pd.DataFrame) -> pd.Series:
    ba_series = df["bank_account"] if "bank_account" in df.columns else pd.Series([""] * len(df))
    cc_series = df["bank_cc_num"] if "bank_cc_num" in df.columns else pd.Series([""] * len(df))

    def _build(ba_val, cc_val) -> str:
        key = _normalized_cc_key(cc_val)
        mapped = BANK_CC_NUM_TO_ACCOUNT_CC.get(key) if key else None
        if mapped:
            return mapped

        ba_str = "" if pd.isna(ba_val) else str(ba_val).strip()
        cc_str = "" if pd.isna(cc_val) else str(cc_val).strip()
        return f"{ba_str} {cc_str}".strip()

    return pd.Series((_build(ba, cc) for ba, cc in zip(ba_series, cc_series)), index=df.index, dtype="object")

def _derive_realme_client_name(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="object")
    if series.empty:
        return series

    def _lookup(val):
        if pd.isna(val):
            return pd.NA
        key = str(val).strip().upper()
        return REALME_CLIENT_NAME_MAP.get(key, pd.NA)

    return series.apply(_lookup)

def _apply_resolver(df: pd.DataFrame, col: str, infer_fn: Callable[[dict], Tuple[str, str]]) -> pd.DataFrame:
    """
    Apply a single rulebook over the DataFrame row-by-row.
    Adds 3 lineage columns next to the resolved value:
      - <col>_rule_tag (str)
      - <col>_confidence (float) -> 1.0 if matched, else 0.0
      - <col>_source (str)       -> 'rule' if matched, else 'unknown'
    """
    value_col = col
    tag_col = f"{col}_rule_tag"
    conf_col = f"{col}_confidence"
    src_col = f"{col}_source"

    values: List[str] = []
    tags: List[str] = []
    confs: List[float] = []
    srcs: List[str] = []

    # Iterate rows once (fast enough for weekly batches)
    for _, row in df.iterrows():
        v, tag = infer_fn(row.to_dict())
        if v and v != "UNKNOWN":
            values.append(v)
            tags.append(tag or "")
            confs.append(1.0)
            srcs.append("rule")
        else:
            # Normalize the fallback so downstream always gets a string
            values.append("UNKNOWN")
            tags.append("")
            confs.append(0.0)
            srcs.append("unknown")

    out = df.copy()
    out[value_col] = values
    out[tag_col] = tags
    out[conf_col] = confs
    out[src_col] = srcs
    return out

def categorize_week(gold_week: pd.DataFrame) -> pd.DataFrame:
    """
    Universal categorization entrypoint.
    Takes the weekly gold dataframe and appends resolved fields for all configured rulebooks.
    Returns a frame aligned to CATEG_COLS (missing columns will be added as NA).
    """
    if gold_week is None or gold_week.empty:
        return pd.DataFrame(columns=CATEG_COLS)

    out = gold_week.copy()

    if "extended_description" in out.columns:
        out["extended_description"] = out["extended_description"].fillna("")
    else:
        out["extended_description"] = ""

    if "description" in out.columns:
        out["description"] = _normalize_description(out["description"])
    else:
        out["description"] = ""

    # Derived presentation helpers
    out["bank_account_cc"] = _map_bank_account_cc(out)

    wk_num = out["week_num"] if "week_num" in out.columns else pd.Series([pd.NA] * len(out), dtype="Int64")
    out["week_label"] = "Week " + wk_num.astype("Int64").astype(str)

    # Apply each rulebook in order
    for col, infer_fn in RESOLVERS:
        out = _apply_resolver(out, col, infer_fn)
        if col == "payee_vendor":
            out = postprocess_payee_vendor(out)

    if "entity_qbo" not in out.columns:
        out["entity_qbo"] = pd.NA
    out["realme_client_name"] = _derive_realme_client_name(out["entity_qbo"])

    # Ensure final schema (adds NA for any missing columns)
    out = _ensure_columns(out, CATEG_COLS)

    # Return only the expected output columns (preserve order)
    return out[CATEG_COLS].copy()
