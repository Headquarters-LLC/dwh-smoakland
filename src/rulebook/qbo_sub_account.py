# src/rulebook/qbo_sub_account.py
from __future__ import annotations
from typing import Tuple, List, Dict
import re

"""
qbo_sub_account rulebook
------------------------
- Contract (row-level): infer(row) -> (value, rule_tag)
  * value:    resolved qbo_sub_account (str) or "UNKNOWN"
  * rule_tag: provenance "qbo_sub_account@<version>#<rule_id>" or "" if unknown

"""

RULEBOOK_NAME = "qbo_sub_account"
RULEBOOK_VERSION = "2025.10.05"   # bump when rules change
UNKNOWN = "UNKNOWN"

_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    (re.compile(r'(?i)\b(?:GB\ 1875|DAMA\ 5597|SOCAL|DAMA\ 2370|KP\ 6852|HAH|DAMA\ 7403|SSC|EWB\ 8452|NBCU\ 2211|NBCU\ 2035|NBCU\ SWD|SWD|DMD|CC\ 71000|EWB\ 3447|GB\ 0073|GB\ 0074|CC\ 8305|CC\ 8267)\b'), ''),
]

_SOURCE_COLS: List[str] = [
    "bank_cc_num", "entity_qbo",
]

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.upper().strip()
    return " ".join(s.split())

def _concat_row(row: Dict) -> str:
    parts: List[str] = []
    for c in _SOURCE_COLS:
        v = row.get(c, "")
        if v is None:
            v = ""
        parts.append(str(v))
    return _normalize_text(" | ".join([p for p in parts if p]))

def _rule_tag(i: int, custom: str | None) -> str:
    rid = custom if (custom and custom.strip()) else str(i)
    return f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rid}"

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def infer(row: Dict) -> Tuple[str, str]:
    """
    Returns:
      (value, rule_tag) where value is the resolved qbo_sub_account or "UNKNOWN".
      rule_tag is "qbo_sub_account@<version>#<rule_id>" or "" if unknown.
    """
    text = _concat_row(row)
    if not text:
        return (UNKNOWN, "")

    for i, rule in enumerate(_RULES, start=1):
        # Support 2-tuple or 3-tuple rules
        if len(rule) == 3:
            pat, resolved, custom_id = rule
        else:
            pat, resolved = rule
            custom_id = None

        # Allow compiled regex or raw string
        if hasattr(pat, "search"):
            matched = bool(pat.search(text))
        else:
            matched = bool(re.search(str(pat), text))

        if matched:
            value = (resolved or "").strip()
            if not value:
                return (UNKNOWN, "")
            return (value, _rule_tag(i, custom_id))

    return (UNKNOWN, "")

def apply_rules_df(df) -> "pd.DataFrame":  # type: ignore
    """
    Vectorized convenience: adds
      - qbo_sub_account
      - qbo_sub_account_rule_tag
      - qbo_sub_account_confidence
      - qbo_sub_account_source
    """
    import pandas as pd  # local import to keep module light
    values: List[str] = []
    tags: List[str] = []
    confs: List[float] = []
    srcs: List[str] = []

    for _, row in df.iterrows():
        v, tag = infer(row.to_dict())
        if v != UNKNOWN:
            values.append(v)
            tags.append(tag)
            confs.append(1.0)
            srcs.append("rule")
        else:
            values.append(UNKNOWN)
            tags.append("")
            confs.append(0.0)
            srcs.append("unknown")

    out = df.copy()
    out["qbo_sub_account"] = values
    out["qbo_sub_account_rule_tag"] = tags
    out["qbo_sub_account_confidence"] = confs
    out["qbo_sub_account_source"] = srcs
    return out
