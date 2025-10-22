# src/rulebook/entity_qbo.py
from __future__ import annotations
from typing import Tuple, List, Dict
import re

"""
entity_qbo rulebook
-------------------
- Contract: infer(row) -> (value, rule_tag)
  * value:   resolved Entity/QBO company code (str) or "UNKNOWN"
  * rule_tag: provenance "entity_qbo@<version>#<rule_id>" or "" if unknown

"""

RULEBOOK_NAME = "entity_qbo"
RULEBOOK_VERSION = "2025.10.03"   # <-- bump when rules change
UNKNOWN = "UNKNOWN"

_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    (re.compile(r'(?i)\b(?:NBCU\ 2035|CC\ 8202|GB\ 0073|DAMA\ 2370|CASHLESS\ ATM|PLIVO|INTELLIWORX\ PH|EWB\ 3447|ARCO|ODOO\ B2B|EMPYREAL\ ENTERPRISES|DAMA\ FINANCIAL\ 2370|CC\ 71000|WEINSTEIN\ LOCAL|HONEY\ BUCKET|ECHTRAI\ LLC|SUBLIME\ MACHINING\ INC|HUMANA|PS\ ADMINISTRATORS|ATLAS)\b'), 'DMD'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:KP\ 6852|OSS|DNS|KEYPOINT\ CREDIT\ UNION|SACRAMENTO\ RENT|WORLDWIDE\ ATM|SWITCH\ COMMERCE|XEVEN\ SOLUTIONS|SQUARE\ INC|OU\ SAEFONG|NEW\ VENTURE\ ESCROW|FRANCHISE\ TAX|E\.T\.I\.\ FINANCIAL)(?![A-Z0-9])'), 'HAH'),
    (re.compile(r'(?i)\b(?:DAMA\ 5597)\b'), 'SOCAL'),
    (re.compile(r'(?i)\b(?:EWB\ 8452)\b'), 'SSC'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:NBCU\ 2211|GB\ 1875|CINTAS\ CORP|SOUTHWEST|B2B|ATT|MEDIWASTE\ DISPOSAL\ LLC|CC\ 9551|ADT|PASS|MCGRIFF|E\-Z|FLOR\ X|PRIMO\ WATER|ECOGREENINDUSTRIES|SECURITY\ MARKETING\ KING|ROOT\ SCIENCES|EARLYBRD|JOINHOMEBASE|UPGBOARD)(?![A-Z0-9])'), 'SUB'),
    (re.compile(r'(?i)\b(?:NBCU\ SWD)\b'), 'SWD'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CC\ 7639|LHI|HAPPY\ CABBAGE|SACRAMENTO\ MUNICIPAL|CANVA|MSFT|SPECTRUM|COMETCHAT|CC\ 8305|ALIBABA\.COM|GODADDY\.COM|GOOGLE\ ADS|DOORDASH|AMAZON\ WEB\ SERVICES|AIRCALL|MESA\ OUTDOOR\ BILLB|CC\ 8267|GOOGLE\ CLOUD|ZOOM|CA\ SECRETARY\ OF\ STATE)(?![A-Z0-9])'), 'TPH'),
]


_SOURCE_COLS: List[str] = [
    "bank_cc_num", "payee_vendor",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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
    """
    Build a stable provenance tag:
      entity_qbo@<version>#<id>
    where <id> is either an explicit label (custom) or the 1-based index.
    """
    rid = custom if (custom and custom.strip()) else str(i)
    return f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rid}"

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def infer(row: Dict) -> Tuple[str, str]:
    """
    Row-level API used by the universal resolver.
    Returns:
      (value, rule_tag) where value is the resolved entity_qbo or "UNKNOWN".
      rule_tag is "entity_qbo@<version>#<rule_id>" or "" if unknown.
    """
    text = _concat_row(row)
    if not text:
        return (UNKNOWN, "")

    for i, rule in enumerate(_RULES, start=1):
        # Allow (pattern, value) or (pattern, value, custom_id)
        if len(rule) == 3:
            pat, value, custom_id = rule  # type: ignore[misc]
        else:
            pat, value = rule             # type: ignore[misc]
            custom_id = None

        if pat.search(text):
            v = (value or "").strip()
            if not v:
                return (UNKNOWN, "")
            return (v, _rule_tag(i, custom_id))

    return (UNKNOWN, "")

def apply_rules_df(df) -> "pd.DataFrame":  # type: ignore
    """
    Vectorized convenience that produces 4 columns:
      entity_qbo, entity_qbo_rule_tag, entity_qbo_confidence, entity_qbo_source
    (Same semantics the universal resolver will add.)
    """
    import pandas as pd  # local import to keep this module light if pandas isn't needed

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
    out["entity_qbo"] = values
    out["entity_qbo_rule_tag"] = tags
    out["entity_qbo_confidence"] = confs
    out["entity_qbo_source"] = srcs
    return out
