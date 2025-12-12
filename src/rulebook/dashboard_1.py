# src/rulebook/dashboard_1.py
from __future__ import annotations
from typing import Iterable, Tuple, List, Dict
import re
import pandas as pd

"""
dashboard_1 rulebook
--------------------
- Contract: infer(row) -> (value, rule_tag)
  * value:   resolved dashboard_1 (str) or "" (blank) when unknown
  * rule_tag: provenance "dashboard_1@<version>#<rule_id>" or "" if unknown

- Paste/refresh regex patterns under _RULES (compiled, ordered by priority).
"""

RULEBOOK_NAME = "dashboard_1"
RULEBOOK_VERSION = "2025.12.13"   # bump when rules change
UNKNOWN = ""                      # fallback must be BLANK, as requested

# ---------------------------------------------------------------------------
# Patterns: list of (compiled_regex, resolved_value, optional_human_readable_id)
# IMPORTANT: Replace the examples below with your real list from
#            dashboard_1_regex_rules.py (_RULES there).
# ---------------------------------------------------------------------------
_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    # --- Administration ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*LEGAL\s+FEES\b'), 'ADMINISTRATION', 'cf-legal-fees'),
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b3\.\s*UTILITIES\b'), 'ADMINISTRATION', 'cf-utilities'),
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b3\.\s*RENT\s+EXPENSE\b'), 'ADMINISTRATION', 'cf-rent'),
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*OFFICE/GENERAL\s+SUPPLIES\b'), 'ADMINISTRATION', 'cf-office'),

    # --- Technology ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*APPS\s*&\s*SOFTWARE\b'), 'TECHNOLOGY', 'cf-apps-software'),

    # --- Finance ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*BANK\s+CHARGES\s*&\s*FEES\b'), 'FINANCE', 'cf-bank-fees'),

    # --- HR ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b4\.\s*PAYROLL\b'), 'HR', 'cf-payroll'),
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*INSURANCE\b'), 'HR', 'cf-insurance'),

    # --- Taxes ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b7\.\s*TAXES\b'), 'TAXES', 'cf-taxes'),

    # --- Marketing ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*MARKETING\b'), 'MARKETING', 'cf-marketing'),

    # --- Inventory / Wholesale ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*VEHICLE\s+EXPENSES\b'), 'INVENTORY - WHOLESALE', 'cf-vehicle'),
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*VEHICLE\s+INSURANCE\b'), 'INVENTORY - WHOLESALE', 'cf-vehicle-ins'),

    # --- Sublime ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b5\.\s*OTHER\s+COGS\b'), 'SUBLIME', 'cf-other-cogs'),

    # --- Executive ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b6\.\s*TRAVEL\s*&\s*MEALS\s+EXPENSES\b'), 'EXECUTIVE', 'cf-travel'),

    # --- Default NO DEPARTMENT for Distribution / Retail / Transfer ---
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b1\.\s*DISTRIBUTION\b'), 'NO DEPARTMENT', 'cf-distribution'),
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b1\.\s*RETAIL\b'), 'NO DEPARTMENT', 'cf-retail'),
    (re.compile(r'(?i)\bCF_ACCOUNT:[^|]*\b2\.\s*TRANSFER\b'), 'NO DEPARTMENT', 'cf-transfer'),

    # --- New York overrides ---
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bMONTHLY\s+MAINTENANCE\s+FEE\b.*\bBANK_ACCOUNT_CC:[^|]*\bDAMA\s+7403\b'), 'NEW YORK', 'mmf-dama-7403-ny'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bCHANG\s+YI\b'), 'NEW YORK', 'chang-yi-ny'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bE-Z\*?PASS\b'), 'NEW YORK', 'ezpass-ny'),

    # --- Driver Operations - Bay ---
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bFASTRAK\b'), 'DRIVER OPERATIONS - BAY', 'fastrak-bay'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bONFLEET\b'), 'DRIVER OPERATIONS - BAY', 'onfleet-bay'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bINTELLIWORX\s+PH\b'), 'DRIVER OPERATIONS - BAY', 'intelliworx-bay'),

    # --- Inventory Wholesale explicit ---
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bALAMO\s+TOLL\b.*\bBANK_ACCOUNT_CC:[^|]*\bCC\s+71000\b'), 'INVENTORY - WHOLESALE', 'alamo-toll-71000'),

    # Legacy rules
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
    "payee_vendor", "bank_account_cc", "amount", "cf_account",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.upper().strip()
    return " ".join(s.split())

_POSTPROCESS_SOURCE = "postprocess"

def _concat_row(row: Dict) -> str:
    parts: List[str] = []
    for c in _SOURCE_COLS:
        v = row.get(c, "") or ""
        v = str(v)
        if v.strip():
            parts.append(f"{c.upper()}: {v}")
    return _normalize_text(" | ".join(parts))

def postprocess(df: "pd.DataFrame") -> "pd.DataFrame":  # type: ignore
    if df is None or df.empty:
        return df

    need = {"payee_vendor", "bank_account_cc", "amount", "dashboard_1"}
    if not need.issubset(df.columns):
        return df

    out = df.copy()
    amt = pd.to_numeric(out["amount"], errors="coerce")
    payee = out["payee_vendor"].fillna("").astype(str).str.upper()
    acct = out["bank_account_cc"].fillna("").astype(str).str.upper()
    cf = out["cf_account"].fillna("").astype(str).str.upper() if "cf_account" in out.columns else pd.Series([""] * len(out))

    def stamp(mask, value, rule_id: str):
        if not mask.any():
            return
        out.loc[mask, "dashboard_1"] = value
        if "dashboard_1_rule_tag" in out.columns:
            out.loc[mask, "dashboard_1_rule_tag"] = f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rule_id}"
        if "dashboard_1_source" in out.columns:
            out.loc[mask, "dashboard_1_source"] = _POSTPROCESS_SOURCE
        if "dashboard_1_confidence" in out.columns:
            out.loc[mask, "dashboard_1_confidence"] = 1.0

    aer = payee.eq("AEROPAY")
    stamp(aer & acct.eq("DAMA 5597") & (amt > 0), "NO DEPARTMENT", "pp-aeropay-dama-5597-pos")
    stamp(aer & acct.eq("DAMA 5597") & (amt < 0), "FINANCE", "pp-aeropay-dama-5597-neg")

    stamp(aer & acct.eq("KP 6852") & (amt > 0), "NO DEPARTMENT", "pp-aeropay-kp-6852-pos")
    stamp(aer & acct.eq("KP 6852") & (amt < 0), "FINANCE", "pp-aeropay-kp-6852-neg")

    stamp(aer & acct.eq("NBCU 2035") & (amt > 0), "NO DEPARTMENT", "pp-aeropay-nbcu-2035-pos")
    stamp(aer & acct.eq("NBCU 2035") & (amt < 0), "FINANCE", "pp-aeropay-nbcu-2035-neg")

    stamp(aer & acct.eq("NBCU SWD") & (amt > 0), "NO DEPARTMENT", "pp-aeropay-nbcu-swd-pos")
    stamp(aer & acct.eq("NBCU SWD") & (amt < 0), "FINANCE", "pp-aeropay-nbcu-swd-neg")

    arm = payee.eq("ARMORED CAR")
    stamp(arm & (amt > 0), "NO DEPARTMENT", "pp-armored-car-pos")
    stamp(arm & (amt < 0), "FINANCE", "pp-armored-car-neg")

    bud = payee.eq("BUD TECHNOLOGY")
    stamp(bud & (amt > 0), "NO DEPARTMENT", "pp-bud-technology-pos")
    stamp(bud & (amt < 0), "SUBLIME", "pp-bud-technology-neg")

    oss = payee.eq("OSS")
    stamp(oss & (amt > 0), "NO DEPARTMENT", "pp-oss-pos")
    stamp(oss & (amt < 0), "FINANCE", "pp-oss-neg")

    odoo = payee.eq("ODOO B2B") & acct.eq("EWB 3447") & (amt > 0)
    stamp(odoo, "NO DEPARTMENT", "pp-odoo-b2b-ewb-3447-pos")

    blaze = payee.eq("BLAZE") & (amt.abs() == 1050.00)
    stamp(blaze, "TECHNOLOGY", "pp-blaze-1050")

    xev = payee.eq("XEVEN SOLUTIONS") & (amt.abs() == 3200.00)
    stamp(xev, "MARKETING", "pp-xeven-3200")

    return out

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
    return postprocess(out)
