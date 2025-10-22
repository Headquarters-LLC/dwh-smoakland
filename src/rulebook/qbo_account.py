# src/rulebook/qbo_account.py
from __future__ import annotations
from typing import Tuple, List, Dict
import re

"""
qbo_account rulebook
--------------------
- Contract: infer(row) -> (value, rule_tag)
  * value:    resolved QBO Account (str) or "" if unknown
  * rule_tag: provenance "qbo_account@<version>#<rule_id>" or "" if unknown

- To refresh rules:
  regenerate from your candidate CSV and paste patterns into _RULES.
"""

RULEBOOK_NAME = "qbo_account"
RULEBOOK_VERSION = "2025.10.05"      # <-- bump when rules change
UNKNOWN = ""                         # <-- fallback is blank for unknowns

# ---------------------------------------------------------------------------
# Patterns: list of (compiled_regex, resolved_value, optional_rule_id_label)
# If no label is provided, the ordinal index (1-based) is used for the tag.
# ---------------------------------------------------------------------------
_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    (re.compile(r'(?i)(?<![A-Z0-9])(?:SALES\ AND\ USE|EXCISE|CDTFA|CA\ DEPT\ TAX\ FEE|EPMT|PRE\-AUTHORIZED\ ACH\ DEBIT)(?![A-Z0-9])'), 'CDTFA TAXES'),
    (re.compile(r'(?i)\b(?:PAYNW|MG\ TRUST|SPENCER\ YOUNG\ LAW\ PC|ATLAS)\b'), 'DELIVERY COGS:LABOR COST'),
    (re.compile(r'(?i)\b(?:GUSTO|PAYROLLSYS|PAYROLL\ SYS|PAYROLL\ SERVICE)\b'), 'DIRECT LABOR COST'),
    (re.compile(r'(?i)\b(?:GB\ 1875|GUSTO|PAYROLLSYS|PAYROLL\ SYS|PAYROLL\ SERVICE)\b'), 'DIRECT LABOR COSTS'),
    (re.compile(r'(?i)\b(?:MCGRIFF)\b'), 'COST OF PRODUCTION:DIRECT LABOR COSTS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:DNS|SWITCH\ ?COMMERCE|SQUARE\ ?INC|OU\ SAEFONG|CANNABIS\ ?SALES|SMOAKLAND\ ?CANNABIS\ SALES)(?![A-Z0-9])'), 'DELIVERY REVENUE:CANNABIS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ODOO\ B2B|CURRENCY\ AND\ COIN\ DEPOSITED|ALKHEMIST\ DM\ LLC|BRYANT\ AND\ GILBE|SUMMIT\ LOCATIONS\ TRANSFER\ 240104\ 4ZN2A7PJMYQD|SCZZ\ COLLECTIVE|CIRCLE\ MUSKRAT|PCF\ DISTRO|TIGRANLEVONANDLA|KIM\ INVESTMENTS|HAPPY\ PORT|THE\ NORDHOFF\ COM|AREA\ 29\ LLC|ANTIOCH\ LLC|OTC\ VAN\ NUYS|MONTEREY\ LLC|MANTECA\ LLC|ASHS\ FIRST\ LLC|THE\ HEALING\ CENTER|OTC\ INDIO\ LLC|MOM\ GO\ LLC|SONOMA\ CHO\ LLC|BANYAN\ TREE|SYLMAR\ LLC|HILIFE\ GROUP|CALITA\ INC\.)(?![A-Z0-9])'), 'DISTRIBUTION REVENUE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CASHLESS\ ?ATM|FHL\ WEB\ INC|CCD\ SETTLEMENT|AEROPAY|STRONGHOLD|SWITCH\ ?COMMERCE|ODOO\ ?B2B|DEPOSITBRIDGEPLUS)(?![A-Z0-9])'), 'DELIVERY REVENUE'),
    (re.compile(r'(?i)(?:(?=.*(?:TPH\ ?786|TPH786))(?=.*(?:KP\ ?6852)).*|TPH\ ?786|TPH786|KP\ ?6852|PLIVO|ECHTRAI\ LLC|MEDIAJEL|DEEP\ ROOTS|JPS\ INC\.|TWILIO|CALLRAIL\ INC|TRINITY\ CONSULTING\ AND\ HOLDINGS|EUNOIA\ CONSULTING\ GROUP\ LLC|DOUG\ NGO|GOOGLE\ FI)'), 'DUE FROM TPH786'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:NBCU\ ?2035|NORTH\ ?BAY\ ?CREDIT\ ?UNION)(?![A-Z0-9])'), 'NORTH BAY CREDIT UNION CHECKING 2035'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EWB\ ?3447|EAST\ ?WEST\ ?BANK\ ?3447)(?![A-Z0-9])'), 'EAST WEST BANK 3447'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EMPYREAL\ ENTERPRISES|CCD\ \-\ FINANCE\ CONTRACT\ PAYMENT\ 79590618\ E\ T|BANK\ ?CHARGES|SERVICE\ ?CHARGE|MONTHLY\ ?MAINTENANCE|NON\ ?\-?SUFFICIENT\ ?FUNDS|NSF\ ?FEE|FEE|FEES|ACFEE|DCFEE)(?![A-Z0-9])'), 'BANK CHARGES & FEES'),
    (re.compile(r'(?i)\b(?:WORLDWIDE\ ATM|KEYPOINT\ CREDIT\ UNION)\b'), 'BANK/ARMOR FEES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:INVENTORY|MERCHANDISE|SUBLIME\ MACHINING|CLOVER\ VALLEY|LC\ 2400|NORTHWEST\ CONFECTIONS\ CA\ LLC|ECOGREENINDUSTRIES|EPOD\ HOLDINGS\ LLC|PATCHWORK\ FARMS|KUSHMEN\ \&\ BAKESFIELD\ ENTERPRISES|SUBLIME\ MACHINING\ INC|FAMILY\ FLORALS|ADCHEM\ LLC|NORTHWEST\ CONFECTIONS|EARTHWISE\ PACKAGING|OLD\ PAL\ LLC|KUSHMEN\ \&\ BAKEFIELDS|HIGHLAND\ PARK\ PATIENT\ COLLECTIVE\ INC\.|THE\ WEBSTAURANT\ STORE\ INC|LUCKY\ FAMILY\ BRAND|HIGHLAND\ PARK\ PATIENT\ COLLECTIVE|CUSTOM\ CONES\ USA|COST\ OF\ INVENTORY|COGS)(?![A-Z0-9])'), 'COST OF INVENTORY'),
    (re.compile(r'(?i)\b(?:ISLAMIC\ CENTER\ OF\ VENTURA\ COUNTY)\b'), 'SMOAKLAND SOCAL EXPENSE:BANK CHARGES & FEES'),
    (re.compile(r'(?i)\b(?:LAW\ OFFICE\ OF\ CARLOS\ JATO)\b'), 'TPH786 EXPENSES:LEGAL FEES'),
    (re.compile(r'(?i)\b(?:HAPPY\ DAYS\ DISPENSARY\ INC|NSEW\ TRADING\ COMPANY\ LLC|GUARDIAN\ WELLNESS|TWENTY8GRAMZ|NUCLEUS\ DISPENSARY|FRESHLY\ BAKED\ NYC|EXIT\ 31\ EXOTIC|DAMA\ 7403)\b'), 'NY DISTRIBUTION REVENUE'),
    (re.compile(r'(?i)\b(?:MORGAN\ AT\ PROVOST|SPIRIT\ AIRLINES|MORGAN\ AT\ PROVOST\ SQUARE|UNITED)\b'), 'TPH786 EXPENSES:TRAVEL EXPENSE'),
    (re.compile(r'(?i)\b(?:MEDIWASTE\ DISPOSAL\ LLC)\b'), 'OVERHEAD COSTS:UTILITIES EXPENSE:WASTE DISPOSAL'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:AVIS\ FEE|ENTERPRISE\ RENT\-A\-CAR)(?![A-Z0-9])'), 'SMOAKLAND DISTRIBUTION EXPENSES:DISTRIBUTION VEHICLE EXPENSES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:RA\ \&\ BL\ BEGGS\ TRUST)(?![A-Z0-9])'), 'SMOAKLAND SOCAL EXPENSE:RENT EXPENSE'),
    (re.compile(r'(?i)\b(?:CALIFORNIA\ EXTRACTION|CHEFSTORE)\b'), 'COST OF PRODUCTION:PRODUCTION SUPPLIES'),
    (re.compile(r'(?i)\b(?:LHI|HAPPY\ CABBAGE|GOOGLE\ ADS|MESA\ OUTDOOR\ BILLB|SPRINGBIG|DOPE\ MARKETING|SEMRUSH|STEADY\ DEMAND|LB\ MEDIA\ GROUP\ LLC|ALPINE\ IQ|MESA)\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CANVA|MSFT|SPECTRUM|COMETCHAT|GODADDY\.COM|AMAZON\ WEB\ SERVICES|AIRCALL|GOOGLE\ CLOUD|ZOOM|GOOGLE\ GSUITE|TRELLO|BROWSERSTACK|FRONT\ PRIME|DROPBOX|GODADDY|SHOPIFY|DELIGHTED|FRONT\ GROWTH|ADOBE|KINDLE\ UNLTD)(?![A-Z0-9])'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS'),
    (re.compile(r'(?i)\b(?:B2B|FLOR\ X|PRIMPATCHARA\ ARORA|MARISA\ K\ BADUA)\b'), 'CANNABIS SALES'),
    (re.compile(r'(?i)\b(?:0807\ OUTSIDE|PANOCHE\ FOOD\ MART|MT\ DIABLO|OIL\ CHANGER\ HQ|KWIK\ SERV|GAS\ 4\ LESS)\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:FUEL & OIL'),
    (re.compile(r'(?i)\b(?:TOYOTA\ FINANCIAL)\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:REPAIRS AND MAINTENANCE'),
    (re.compile(r'(?i)\b(?:HONOR|HEADQUARTERS\ LLC)\b'), 'TPH786 EXPENSES:CONSULTANT AND PROFESSIONAL FEES'),
    (re.compile(r'(?i)\b(?:WEINSTEIN\ LOCAL|SACRAMENTO\ RENT)\b'), 'FACILITY COSTS:RENT EXPENSE'),
    (re.compile(r'(?i)\b(?:ONLINE\ LABELS)\b'), 'DISTRIBUTION COGS:PACKAGING'),
    (re.compile(r'(?i)\b(?:GREENBAX\ 0073)\b'), 'GREENBAX 0073'),
    (re.compile(r'(?i)\b(?:MOMENTUM\ IOT|ONFLEET)\b'), 'DELIVERY COGS:VEHICLE COMPLIANCE'),
    (re.compile(r'(?i)\b(?:HONEY\ BUCKET)\b'), 'FACILITY COSTS:SECURITY EXPENSE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:BUTTONWILLOW|UNION|FUEL\ \&\ OIL|SHELL|CHEVRON|76\ |AMOCO|VALERO)(?![A-Z0-9])'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ALIBABA\.COM)(?![A-Z0-9])'), 'DUE FROM DMD'),
    (re.compile(r'(?i)\b(?:PRIMO\ WATER)\b'), 'OVERHEAD COSTS:UTILITIES EXPENSE:WATER'),
    (re.compile(r'(?i)\b(?:READYREFRESH)\b'), 'TPH786 EXPENSES:OFFICE EXPENSES'),
    (re.compile(r'(?i)\b(?:CC\ PAYMENT)\b'), 'CC 8202'),
    (re.compile(r'(?i)\b(?:ADT|EVERON\ LLC)\b'), 'OVERHEAD COSTS:SECURITY EXPENSE'),
    (re.compile(r'(?i)\b(?:TWITCHTVDON)\b'), 'TPH786 EXPENSES:CONSULTANT FEES'),
    (re.compile(r'(?i)\b(?:NEXT\ WAVE)\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:AUTO INSURANCE'),
    (re.compile(r'(?i)\b(?:MARCO\ FEI)\b'), 'SMOAKLAND NEW YORK EXPENSE:RENT EXPENSE'),
    (re.compile(r'(?i)\b(?:STONEMARK)\b'), 'FACILITY COSTS:PROPERTY INSURANCE'),
    (re.compile(r'(?i)\b(?:FORMSWIFT)\b'), 'TPH786 EXPENSES:TAX SOFTWARE'),
    (re.compile(r'(?i)\b(?:APPFOLIO|INC)\b'), 'DELIVERMD EXPENSES:OFFICE/GENERAL SUPPLIES'),
    (re.compile(r'(?i)\b(?:INFINITY)\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:VEHICLE INSURANCE'),
    (re.compile(r'(?i)\b(?:GB\ 0074)\b'), 'TPH786 EXPENSES:BANK CHARGES'),
    (re.compile(r'(?i)\b(?:DOORDASH)\b'), 'TPH786 EXPENSES:MEALS / LUNCHES'),
    (re.compile(r'(?i)\b(?:EARLYBRD)\b'), 'TRAVEL, AUTO & MEALS EXPENSES'),
    (re.compile(r'(?i)\b(?:ALIBABA)\b'), 'COST OF PRODUCTION:PACKAGING'),
    (re.compile(r'(?i)\b(?:FASTRAK)\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:OTHER EXPENSES'),
    (re.compile(r'(?i)\b(?:WILTON)\b'), 'OFFICE/GENERAL SUPPLIES'),
    (re.compile(r'(?i)\b(?:HISIG)\b'), 'TPH786 EXPENSES:INSURANCE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:PASS|E\-Z)(?![A-Z0-9])'), 'SMOAKLAND NEW YORK EXPENSE:VEHICLE EXPENSE'),
]

# We concatenate the same canonical fields we’ve been using across rulebooks.
_SOURCE_COLS: List[str] = [
    "bank_cc_num", "payee_vendor", "entity_qbo",
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
    rid = custom if (custom and custom.strip()) else str(i)
    return f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rid}"

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def infer(row: Dict) -> Tuple[str, str]:
    """
    Row-level API used by the universal resolver.
    Returns:
      (value, rule_tag) where value is the resolved qbo_account or "UNKNOWN".
      rule_tag is "qbo_account@<version>#<rule_id>" or "" if unknown.
    """
    text = _concat_row(row)
    if not text:
        return (UNKNOWN, "")

    for i, rule in enumerate(_RULES, start=1):
        # Support both 2-tuple and 3-tuple rules
        if len(rule) == 3:
            pat, val, custom_id = rule  # pattern, resolved_value, custom_id
        else:
            pat, val = rule
            custom_id = None

        # Handle both compiled regex and raw string patterns
        if hasattr(pat, "search"):
            matched = bool(pat.search(text))
        else:
            matched = bool(re.search(str(pat), text))

        if matched:
            value = (val or "").strip()
            if not value:
                # Blank value = unresolved (UNKNOWN)
                return (UNKNOWN, "")
            return (value, _rule_tag(i, custom_id))

    # No matches found
    return (UNKNOWN, "")

def apply_rules_df(df) -> "pd.DataFrame":  # type: ignore
    """
    Vectorized helper that adds:
      qbo_account, qbo_account_rule_tag, qbo_account_confidence, qbo_account_source
    """
    import pandas as pd

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
    out["qbo_account"] = values
    out["qbo_account_rule_tag"] = tags
    out["qbo_account_confidence"] = confs
    out["qbo_account_source"] = srcs
    return out
