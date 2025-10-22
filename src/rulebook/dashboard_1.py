# src/rulebook/dashboard_1.py
from __future__ import annotations
from typing import Iterable, Tuple, List, Dict
import re

"""
dashboard_1 rulebook
--------------------
- Contract: infer(row) -> (value, rule_tag)
  * value:   resolved dashboard_1 (str) or "" (blank) when unknown
  * rule_tag: provenance "dashboard_1@<version>#<rule_id>" or "" if unknown

- Paste/refresh regex patterns under _RULES (compiled, ordered by priority).
"""

RULEBOOK_NAME = "dashboard_1"
RULEBOOK_VERSION = "2025.10.04"   # bump when rules change
UNKNOWN = ""                      # fallback must be BLANK, as requested

# ---------------------------------------------------------------------------
# Patterns: list of (compiled_regex, resolved_value, optional_human_readable_id)
# IMPORTANT: Replace the examples below with your real list from
#            dashboard_1_regex_rules.py (_RULES there).
# ---------------------------------------------------------------------------
_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    (re.compile(r'(?i)(?<![A-Z0-9])(?:COMCAST|DOORDASH|T\-MOBILE|BAY\ ALARM\ COMPANY|WEINSTEIN\ LOCAL|HONEY\ BUCKET|ECHTRAI\ LLC|SACRAMENTO\ RENT|ATT|SACRAMENTO\ MUNICIPAL|AD\ ASTRA\ LAW\ GROUP|APPFOLIO|HARBOR\ HR\ LLC|MARCO\ FEI|STONEMARK|LEGALZOOM|SENTRY|SECURITY\ MARKETING\ KING|RA\ \&\ BL\ BEGGS\ TRUST|EDISON)(?![A-Z0-9])'), 'ADMINISTRATION'),
    (re.compile(r'(?i)\b(?:FASTRAK|ONFLEET|MOMENTUM\ IOT|ALAMEDA\ IMPORT|0807\ OUTSIDE|BUTTONWILLOW\ SINCLAIR|ROCKET|PANOCHE\ FOOD\ MART|MT\ DIABLO|CALIFORNIA\ STOP|OIL\ CHANGER\ HQ|KWIK\ SERV|GAS\ 4\ LESS|TOYOTA\ FINANCIAL)\b'), 'DRIVER OPERATIONS - BAY'),
    (re.compile(r'(?i)\b(?:MORGAN\ AT\ PROVOST|SPIRIT\ AIRLINES|MORGAN\ AT\ PROVOST\ SQUARE|UNITED)\b'), 'EXECUTIVE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EMPYREAL\ ENTERPRISES|KEYPOINT\ CREDIT\ UNION|EASTWEST\ BANK|MONTHLY\ MAINTENANCE\ FEE|WORLDWIDE\ ATM|US\ CORPORATION|PAYPAL|CCD\ \-\ FINANCE\ CONTRACT\ PAYMENT\ 79590618\ E\ T|TWITCHTVDON|SIBANNA\ CONSULTANTS)(?![A-Z0-9])'), 'FINANCE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:PAYNW|GUSTO|MATRIX\ TRUST|HUMAN\ INTEREST|HUMANA|MCGRIFF|ATLAS|E\.T\.I\.\ FINANCIAL|NEXT\ WAVE|MG\ TRUST|SPENCER\ YOUNG\ LAW\ PC|HISIG)(?![A-Z0-9])'), 'HR'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ALIBABA\.COM|CLOVER\ VALLEY|LC\ 2400|NORTHWEST\ CONFECTIONS\ CA\ LLC|UNION|ERACTOLL|EPOD\ HOLDINGS\ LLC|KUSHMEN\ \&\ BAKESFIELD\ ENTERPRISES|ONLINE\ LABELS|AVIS\ FEE|FAMILY\ FLORALS|GAWFCO\ WESTLEY|BUTTONWILLOW|IBUDDY\ INC|OIL\ CHANGERS|HIGHLAND\ PARK\ PATIENT\ COLLECTIVE\ INC\.|HIGHLAND\ PARK\ PATIENT\ COLLECTIVE|INFINITY|ENTERPRISE\ RENT\-A\-CAR|GAS\ \&\ FOOD)(?![A-Z0-9])'), 'INVENTORY - WHOLESALE'),
    (re.compile(r'(?i)\b(?:LB\ MEDIA\ GROUP\ LLC|KLAVIYO|MESA|GHOST\ MGMT|PLIVO|TWILIO|JOINTCOMMERCE|LHI|GOOGLE\ ADS|HAPPY\ CABBAGE|ALPINE\ IQ|SPARKS\ MARKETING|MESA\ OUTDOOR\ BILLB|DEEP\ ROOTS|ZENITH\ BILLBOARD|SPRINGBIG|DOPE\ MARKETING|SEMRUSH|MEDIAJEL|STEADY\ DEMAND)\b'), 'MARKETING'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:PASS|E\-Z)(?![A-Z0-9])'), 'NEW YORK'),
    (re.compile(r'(?i)\b(?:SWITCH\ COMMERCE|STRONGHOLD|TUV\ INVESTMENTS|ODOO\ B2B|CC\ 9551|CC\ 8305|LEDGER\ GREEN|CC\ 8202|CASHLESS\ ATM|DELIVERMD|CC\ 8267|WEST\ TOWN\ BNK|DNS|HAH|GAIN\ FCU|CURRENCY\ AND\ COIN\ DEPOSITED|B2B|NBCU\ 2035|EWB\ 3439|EWB\ 3447)\b'), 'NO DEPARTMENT'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CINTAS\ CORP|MANDELA\ PARTNERS\ LLC|ALIBABA|HARRENS\ LAB\ INC\.|ADT|PRIMO\ WATER|ECOGREENINDUSTRIES|ROOT\ SCIENCES|SP\ PRM\ FILTRATION|CUSTOM\ CONES\ USA|MEDIWASTE\ DISPOSAL\ LLC|WILTON|NORTHWEST\ CONFECTIONS|EARTHWISE\ PACKAGING|MODERNIST\ PANTRY|HONGKONG\ PORTER|EVERON\ LLC|CALIFORNIA\ EXTRACTION|ULINE|THE\ WEBSTAURANT\ STORE\ INC)(?![A-Z0-9])'), 'SUBLIME'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ADOBE|QUICKBOOKS|BLAZE|VERCEL|GITHUB|AMAZON\ WEB\ SERVICES|AIRCALL|GOOGLE\ CLOUD|GOOGLE\ GSUITE|BROWSERSTACK|CANVA|DOUG\ NGO|MSFT|CALLRAIL\ INC|SPECTRUM|SAMSARA|TRELLO|COMETCHAT|GODADDY\.COM|ZOOM)(?![A-Z0-9])'), 'TECHNOLOGY'),
]

# Fields concatenated to build the searchable text.
# Order matters; we include outputs of previous rulebooks to simplify patterns.
_SOURCE_COLS = [
    "payee_vendor",
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
      dashboard_1@<version>#<id>
    where <id> is either the explicit label (custom) or the 1-based index.
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
      (value, rule_tag) where value is the resolved dashboard_1 or "" (blank).
      rule_tag is "dashboard_1@<version>#<rule_id>" or "" if unknown.
    """
    text = _concat_row(row)
    if not text:
        return ("", "")

    for i, rule in enumerate(_RULES, start=1):
        # Accept (pattern, value) or (pattern, value, custom_id)
        if len(rule) == 3:
            pat, resolved_value, custom_id = rule  # type: ignore[misc]
        else:
            pat, resolved_value = rule            # type: ignore[misc]
            custom_id = None

        if pat.search(text):
            value = (resolved_value or "").strip()
            # If the rule maps to blank, treat as unknown (no tag)
            if not value:
                return ("", "")
            return (value, _rule_tag(i, custom_id))

    return ("", "")


def apply_rules_df(df) -> "pd.DataFrame":  # type: ignore
    """
    Vectorized convenience that produces 4 columns:
      dashboard_1, dashboard_1_rule_tag, dashboard_1_confidence, dashboard_1_source
    """
    import pandas as pd  # local import to keep this module light if pandas isn't needed

    values: List[str] = []
    tags: List[str] = []
    confs: List[float] = []
    srcs: List[str] = []

    for _, row in df.iterrows():
        v, tag = infer(row.to_dict())
        if v != "":
            values.append(v)
            tags.append(tag)
            confs.append(1.0)
            srcs.append("rule")
        else:
            values.append("")     # blank fallback
            tags.append("")
            confs.append(0.0)
            srcs.append("unknown")

    out = df.copy()
    out["dashboard_1"] = values
    out["dashboard_1_rule_tag"] = tags
    out["dashboard_1_confidence"] = confs
    out["dashboard_1_source"] = srcs
    return out
