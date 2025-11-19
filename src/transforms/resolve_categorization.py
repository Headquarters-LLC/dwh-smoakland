#resolve_categorization.py
from __future__ import annotations
from typing import Callable, Dict, List, Tuple
import pandas as pd

# Import each rulebook's row-level API (infer)
from rulebook.payee_vendor import infer as infer_payee_vendor
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
    "bank_account", "subentity", "bank_cc_num",
    "date", "txn_id", "week_num",
    "description", "extended_description",
    "amount", "balance",
    "year",
    # resolved fields (value + lineage for each rulebook)
    "payee_vendor", "payee_vendor_rule_tag", "payee_vendor_confidence", "payee_vendor_source",
    "cf_account",   "cf_account_rule_tag",   "cf_account_confidence",   "cf_account_source",
    "dashboard_1",  "dashboard_1_rule_tag",  "dashboard_1_confidence",  "dashboard_1_source",
    "budget_owner", "budget_owner_rule_tag", "budget_owner_confidence", "budget_owner_source",
    "entity_qbo",   "entity_qbo_rule_tag",   "entity_qbo_confidence",   "entity_qbo_source",
    "qbo_account",  "qbo_account_rule_tag",  "qbo_account_confidence",  "qbo_account_source",
    "qbo_sub_account", "qbo_sub_account_rule_tag", "qbo_sub_account_confidence", "qbo_sub_account_source",
]

def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Add missing columns with NA to guarantee downstream schema."""
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out

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

    # Apply each rulebook in order
    for col, infer_fn in RESOLVERS:
        out = _apply_resolver(out, col, infer_fn)

    # Ensure final schema (adds NA for any missing columns)
    out = _ensure_columns(out, CATEG_COLS)

    # Return only the expected output columns (preserve order)
    return out[CATEG_COLS].copy()
