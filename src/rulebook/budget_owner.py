# src/rulebook/budget_owner.py
from __future__ import annotations
from typing import Tuple, List, Dict
import re

"""
budget_owner rulebook
---------------------
- Contract: infer(row) -> (value, rule_tag)
  * value:   resolved budget owner (str) or "" (blank) if unknown
  * rule_tag: provenance "budget_owner@<version>#<rule_id>" or "" if unknown

- Add/refresh patterns by regenerating from your candidate rules CSV and
  pasting them under _RULES (or import them from a generated module).
"""

RULEBOOK_NAME = "budget_owner"
RULEBOOK_VERSION = "2025.10.05"   # <-- bump when rules change
UNKNOWN = ""                      # blank fallback

# -----------------------------------------------------------------------------
# Patterns: list of (compiled_regex, resolved_owner, optional_rule_id_label)
# Optionally provide a human-friendly id; if omitted the ordinal index is used.
# -----------------------------------------------------------------------------
_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    (re.compile(r'(?i)\b(?:FINANCE)\b'), 'CHANNA'),
    (re.compile(r'(?i)\b(?:MARKETING)\b'), 'DENNIS'),
    (re.compile(r'(?i)\b(?:SUBLIME)\b'), 'DEREK'),
    (re.compile(r'(?i)\b(?:TECHNOLOGY)\b'), 'DOUG'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:DRIVER\ OPERATIONS\ \-\ BAY)(?![A-Z0-9])'), 'ERIC'),
    (re.compile(r'(?i)\b(?:EXECUTIVE)\b'), 'JAY'),
    (re.compile(r'(?i)\b(?:NO\ DEPARTMENT|TAXES|NEW\ YORK)\b'), ''),
    (re.compile(r'(?i)\b(?:ADMINISTRATION)\b'), 'LINDA'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:DRIVER\ OPERATIONS\ \-\ SACRAMENTO)(?![A-Z0-9])'), 'RAJ'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:INVENTORY\ \-\ WHOLESALE)(?![A-Z0-9])'), 'WENDY'),
]


# Which fields we concatenate to form the searchable text.
# Order matters; we include upstream resolutions to simplify patterns.
_SOURCE_COLS = [
    "dashboard_1",
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
    """
    Build a stable provenance tag:
      budget_owner@<version>#<id>
    where <id> is either an explicit label (custom) or the 1-based index.
    """
    rid = custom if (custom and custom.strip()) else str(i)
    return f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rid}"

# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def infer(row: Dict) -> Tuple[str, str]:
    """
    Row-level API used by the universal resolver.
    Returns:
      (value, rule_tag) where value is the resolved budget_owner or "UNKNOWN".
      rule_tag is "budget_owner@<version>#<rule_id>" or "" if unknown.
    """
    text = _concat_row(row)
    if not text:
        return (UNKNOWN, "")

    for i, rule in enumerate(_RULES, start=1):
        # Accept (pattern, owner) or (pattern, owner, custom_id)
        if len(rule) == 3:
            pat, owner, custom_id = rule  # type: ignore[misc]
        else:
            pat, owner = rule            # type: ignore[misc]
            custom_id = None

        if pat.search(text):
            owner = (owner or "").strip()
            if owner:
                return (owner, _rule_tag(i, custom_id))
            # If rule maps to blank, treat as unknown (no tag)
            return (UNKNOWN, "")

    return (UNKNOWN, "")

def apply_rules_df(df) -> "pd.DataFrame":  # type: ignore
    """
    Vectorized convenience that produces 4 columns:
      budget_owner, budget_owner_rule_tag, budget_owner_confidence, budget_owner_source
    (Same semantics the universal resolver will add.)
    """
    import pandas as pd  # local import to keep this module light if pandas isn't needed

    values: List[str] = []
    tags: List[str] = []
    confs: List[float] = []
    srcs: List[str] = []

    for _, row in df.iterrows():
        v, tag = infer(row.to_dict())
        if v:
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
    out["budget_owner"] = values
    out["budget_owner_rule_tag"] = tags
    out["budget_owner_confidence"] = confs
    out["budget_owner_source"] = srcs
    return out
