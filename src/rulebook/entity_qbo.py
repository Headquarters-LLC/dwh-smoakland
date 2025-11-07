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
RULEBOOK_VERSION = "2025.11.07"   # bump: allow cross-field bank_account/bank_cc_num combos and add scoped rules
UNKNOWN = "UNKNOWN"

_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    # --- AEROPAY + bank_account / bank_cc_num (highest priority) ---
    # Aeropay + HAH 6852 → HAH 7 CA
    (re.compile(
        r'(?i)(?:\bAEROPAY\b.*\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bHAH\s+6852\b'
        r'|\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bHAH\s+6852\b.*\bAEROPAY\b)'
    ), 'HAH 7 CA', 'aeropay-hah-6852'),

    # Aeropay + TPH 5597 → SoCal
    (re.compile(
        r'(?i)(?:\bAEROPAY\b.*\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bTPH\s+5597\b'
        r'|\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bTPH\s+5597\b.*\bAEROPAY\b)'
    ), 'SoCal', 'aeropay-tph-5597'),

    # Aeropay + DMD 2035 → SWD
    (re.compile(
        r'(?i)(?:\bAEROPAY\b.*\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bDMD\s+2035\b'
        r'|\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bDMD\s+2035\b.*\bAEROPAY\b)'
    ), 'SWD', 'aeropay-dmd-2035'),

    # --- SUB 9551 scoped overrides (based on dashboard_1, cf_account, or payee_vendor) ---
    # 0) Vendor = City of Sacramento → HAH
    (re.compile(
        r'(?i)(?:\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b.*?\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+SACRAMENTO\b'
        r'|\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+SACRAMENTO\b.*?\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b)'
    ), 'HAH', 'sub-9551-city-of-sacramento'),

    # 1) dashboard_1 = New York → NY
    (re.compile(
        r'(?i)(?:\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b.*?\bDASHBOARD_1:[^|]*\bNEW\s+YORK\b'
        r'|\bDASHBOARD_1:[^|]*\bNEW\s+YORK\b.*?\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b)'
    ), 'NY', 'sub-9551-dashboard-ny'),

    # 2) cf_account = 6. Marketing → TPH
    (re.compile(
        r'(?i)(?:\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b.*?\bCF_ACCOUNT:[^|]*\b6\.\s*MARKETING\b'
        r'|\bCF_ACCOUNT:[^|]*\b6\.\s*MARKETING\b.*?\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b)'
    ), 'TPH', 'sub-9551-cf-marketing'),

    # 3) cf_account = 6. Legal Fees → TPH
    (re.compile(
        r'(?i)(?:\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b.*?\bCF_ACCOUNT:[^|]*\b6\.\s*LEGAL\s+FEES\b'
        r'|\bCF_ACCOUNT:[^|]*\b6\.\s*LEGAL\s+FEES\b.*?\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b)'
    ), 'TPH', 'sub-9551-cf-legal-fees'),

    # 4) payee_vendor contains ATT or FRONTIER COMM CORP WEB → TPH
    (re.compile(
        r'(?i)(?:\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b.*?\bPAYEE_VENDOR:[^|]*\b(?:ATT|FRONTIER\s+COMM\s+CORP\s+WEB)\b'
        r'|\bPAYEE_VENDOR:[^|]*\b(?:ATT|FRONTIER\s+COMM\s+CORP\s+WEB)\b.*?\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b)'
    ), 'TPH', 'sub-9551-vendor-att-frontier'),

    # 5) payee_vendor contains DOORDASH → TPH
    (re.compile(
        r'(?i)(?:\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b.*?\bPAYEE_VENDOR:[^|]*\bDOORDASH\b'
        r'|\bPAYEE_VENDOR:[^|]*\bDOORDASH\b.*?\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b)'
    ), 'TPH', 'sub-9551-vendor-doordash'),

    # --- Standard scoped bank_account / bank_cc_num rules ---
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bTPH\s+8267\b'),  'TPH',  'scoped-cc-8267'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+9551\b'),  'SSC',  'scoped-cc-9551'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bDMD\s+71000\b'), 'SSC',  'scoped-cc-71000'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bTPH\s+7639\b'),  'TPH',  'scoped-cc-7639'),

    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bTPH\s+5597\b'), 'SOCAL', 'scoped-dama-5597'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bTPH\s+7403\b'), 'NY',    'scoped-dama-7403'),

    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bTPH\s+3439\b'),  'TPH',  'scoped-ewb-3439'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bDMD\s+3447\b'),  'SSC',  'scoped-ewb-3447'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSSC\s+8452\b'),  'SSC',  'scoped-ewb-8452'),

    # Fallback for HAH 6852 (without Aeropay) → HAH
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bHAH\s+6852\b'), 'HAH 7 CA', 'scoped-kp-6852'),

    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bDMD\s+2035\b'), 'SWD',  'scoped-nbcu-2035'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSUB\s+2211\b'), 'SSC',  'scoped-nbcu-2211'),
    (re.compile(r'(?i)\b(?:BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bSWD\s+SWD\b'),  'SWD',  'scoped-nbcu-swd'),

    # --- Legacy unscoped patterns (backward compatibility) ---
    (re.compile(r'(?i)\b(?:NBCU\ 2035|CC\ 8202|GB\ 0073|DAMA\ 2370|CASHLESS\ ATM|PLIVO|INTELLIWORX\ PH|EWB\ 3447|ARCO|ODOO\ B2B|EMPYREAL\ ENTERPRISES|DAMA\ FINANCIAL\ 2370|CC\ 71000|WEINSTEIN\ LOCAL|HONEY\ BUCKET|ECHTRAI\ LLC|SUBLIME\ MACHINING\ INC|HUMANA|PS\ ADMINISTRATORS|ATLAS)\b'), 'DMD'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:KP\ 6852|OSS|DNS|KEYPOINT\ CREDIT\ UNION|SACRAMENTO\ RENT|WORLDWIDE\ ATM|SWITCH\ COMMERCE|XEVEN\ SOLUTIONS|SQUARE\ INC|OU\ SAEFONG|NEW\ VENTURE\ ESCROW|FRANCHISE\ TAX|E\.T\.I\.\ FINANCIAL)(?![A-Z0-9])'), 'HAH'),
    (re.compile(r'(?i)\b(?:DAMA\ 5597)\b'), 'SOCAL'),
    (re.compile(r'(?i)\b(?:EWB\ 8452)\b'), 'SSC'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:NBCU\ 2211|GB\ 1875|CINTAS\ CORP|SOUTHWEST|B2B|ATT|MEDIWASTE\ DISPOSAL\ LLC|CC\ 9551|ADT|PASS|MCGRIFF|E\-Z|FLOR\ X|PRIMO\ WATER|ECOGREENINDUSTRIES|SECURITY\ MARKETING\ KING|ROOT\ SCIENCES|EARLYBRD|JOINHOMEBASE|UPGBOARD)(?![A-Z0-9])'), 'SUB'),
    (re.compile(r'(?i)\b(?:NBCU\ SWD)\b'), 'SWD'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CC\ 7639|LHI|HAPPY\ CABBAGE|SACRAMENTO\ MUNICIPAL|CANVA|MSFT|SPECTRUM|COMETCHAT|CC\ 8305|ALIBABA\.COM|GODADDY\.COM|GOOGLE\ ADS|DOORDASH|AMAZON\ WEB\ SERVICES|AIRCALL|MESA\ OUTDOOR\ BILLB|CC\ 8267|GOOGLE\ CLOUD|ZOOM|CA\ SECRETARY\ OF\ STATE)(?![A-Z0-9])'), 'TPH'),
]


_SOURCE_COLS: List[str] = [
    "bank_account", "bank_cc_num", "payee_vendor", "cf_account", "dashboard_1",
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
        v = str(v)
        if v.strip():
            parts.append(f"{c.upper()}: {v}")

    bank_account = str(row.get("bank_account", "") or "").strip()
    bank_cc = str(row.get("bank_cc_num", "") or "").strip()
    if bank_account and bank_cc:
        # Duplicate both labels with the combined token so existing scoped regexes
        # that expect "DMD 2035" style strings continue to match even if the
        # values arrive split across columns.
        parts.append(f"BANK_ACCOUNT: {bank_account} {bank_cc}")
        parts.append(f"BANK_CC_NUM: {bank_account} {bank_cc}")

    return _normalize_text(" | ".join(parts))

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
