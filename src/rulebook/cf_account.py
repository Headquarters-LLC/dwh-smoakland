# src/rulebook/cf_account.py
from __future__ import annotations
from typing import Iterable, Tuple, List, Dict
import re

"""
cf_account rulebook
-------------------
- Contract: infer(row) -> (value, rule_tag)
  * value:   resolved GL/CF account (str) or "UNKNOWN"
  * rule_tag: provenance "cf_account@<version>#<rule_id>" or "" if unknown

- Add/refresh patterns by regenerating from your candidate rules CSV and
  pasting them under _RULES.
"""

RULEBOOK_NAME = "cf_account"
RULEBOOK_VERSION = "2025.10.03"   # <-- update when rules change
UNKNOWN = "UNKNOWN"

# ---------------------------------------------------------------------------
# Patterns: list of (compiled_regex, resolved_cf_account, optional_rule_id_label)
# Optionally provide a human-friendly id; if omitted the ordinal index is used.
# ---------------------------------------------------------------------------
_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CALITA\ INC\.|ALKHEMIST\ DM\ LLC|SUMMIT\ LOCATIONS\ TRANSFER\ 240104\ 4ZN2A7PJMYQD|ODOO\ B2B|PCF\ DISTRO)(?![A-Z0-9])'), '1. DISTRIBUTION'),
    (re.compile(r'(?i)\b(?:CURRENCY\ AND\ COIN\ DEPOSITED|BRYANT\ AND\ GILBE|AREA\ 29\ LLC|ANTIOCH\ LLC|MONTEREY\ LLC|MANTECA\ LLC|OTC\ INDIO\ LLC|MOM\ GO\ LLC|SONOMA\ CHO\ LLC|SYLMAR\ LLC|HILIFE\ GROUP|KIM\ INVESTMENTS|THE\ NORDHOFF\ COM|OTC\ VAN\ NUYS|ASHS\ FIRST\ LLC)\b'), '1. DISTRIBUTION DMD'),
    (re.compile(r'(?i)\b(?:HAPPY\ DAYS\ DISPENSARY\ INC|NSEW\ TRADING\ COMPANY\ LLC|GUARDIAN\ WELLNESS|TWENTY8GRAMZ|NUCLEUS\ DISPENSARY|FRESHLY\ BAKED\ NYC|EXIT\ 31\ EXOTIC)\b'), '1. DISTRIBUTION NY'),
    (re.compile(r'(?i)\b(?:CASHLESS\ ATM|FHL\ WEB\ INC|CCD\ SETTLEMENT)\b'), '1. RETAIL DMD'),
    (re.compile(r'(?i)\b(?:DNS|SWITCH\ COMMERCE|SQUARE\ INC|OU\ SAEFONG)\b'), '1. RETAIL HAH'),
    (re.compile(r'(?i)\b(?:B2B|FLOR\ X|PRIMPATCHARA\ ARORA|MARISA\ K\ BADUA)\b'), '1. SUBLIME'),
    (re.compile(r'(?i)\b(?:CC\ 9551|CC\ 8305|CC\ 8202|CC\ 8267|CC\ PAYMENT)\b'), '2. CC PAYMENT'),
    (re.compile(r'(?i)\b(?:HAH|NBCU\ 2035|DELIVERMD|EWB\ 3439|EWB\ 3447|SUBLIME\ MACHINING|SUBLIME\ MACHINING\ INC|TPH|GREENBAX\ 0073|THE\ PUREST\ HEART\ 786\ INC|HAH\ 7\ CA\ LLC)\b'), '2. TRANSFER'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:WEINSTEIN\ LOCAL|MANDELA\ PARTNERS\ LLC|SACRAMENTO\ RENT|MARCO\ FEI|RA\ \&\ BL\ BEGGS\ TRUST)(?![A-Z0-9])'), '3. RENT EXPENSE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:H\ \&\ L\ PRIVATE\ SECURITY|HONEY\ BUCKET|ADT|BAY\ ALARM\ COMPANY|SECURITY\ MARKETING\ KING|ELITESECURITY|ALARM\ PRO|EVERON\ LLC|ACME\ FIRE\ EXTINGUISHER\ CO)(?![A-Z0-9])'), '3. SECURITY EXPENSE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:WASTE\ MGMT|PG\&E|EBMUD|MEDIWASTE\ DISPOSAL\ LLC|SACRAMENTO\ MUNICIPAL|EDISON|CONED)(?![A-Z0-9])'), '3. UTILITIES'),
    (re.compile(r'(?i)\b(?:PAYNW|MG\ TRUST|SPENCER\ YOUNG\ LAW\ PC|ATLAS)\b'), '4. PAYROLL DMD'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:E\.T\.I\.\ FINANCIAL)(?![A-Z0-9])'), '4. PAYROLL HAH'),
    (re.compile(r'(?i)\b(?:MCGRIFF)\b'), '4. PAYROLL SUB'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CLOVER\ VALLEY|LC\ 2400|NORTHWEST\ CONFECTIONS\ CA\ LLC|EPOD\ HOLDINGS\ LLC|PATCHWORK\ FARMS|KUSHMEN\ \&\ BAKESFIELD\ ENTERPRISES|FAMILY\ FLORALS|IBUDDY\ INC|NORTHWEST\ CONFECTIONS|HIGHLAND\ PARK\ PATIENT\ COLLECTIVE\ INC\.|LUCKY\ FAMILY\ BRAND|HIGHLAND\ PARK\ PATIENT\ COLLECTIVE)(?![A-Z0-9])'), '5. INVENTORY'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:BELCOSTA\ LABS|CINTAS\ CORP|HARRENS\ LAB\ INC\.|ALIBABA\.COM|ALIBABA|ECOGREENINDUSTRIES|ROOT\ SCIENCES|ONLINE\ LABELS|SP\ PRM\ FILTRATION|CUSTOM\ CONES\ USA|WILTON|EARTHWISE\ PACKAGING|OLD\ PAL\ LLC|MODERNIST\ PANTRY|HONGKONG\ PORTER|CALIFORNIA\ EXTRACTION|THE\ WEBSTAURANT\ STORE\ INC|CHEFSTORE|ADCHEM\ LLC)(?![A-Z0-9])'), '5. OTHER COGS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ADOBE|QUICKBOOKS|BLAZE|JOINHOMEBASE|VERCEL|GITHUB|AMAZON\ WEB\ SERVICES|AIRCALL|GOOGLE\ CLOUD|GOOGLE\ GSUITE|BROWSERSTACK|PS\ ADMINISTRATORS|CANVA|MSFT|CALLRAIL\ INC|SPECTRUM|SAMSARA|TRELLO|COMETCHAT|GODADDY\.COM)(?![A-Z0-9])'), '6. APPS & SOFTWARE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:NBCU|EMPYREAL\ ENTERPRISES|KEYPOINT\ CREDIT\ UNION|EASTWEST\ BANK|GREENBAX\ MARKETPLACE|MONTHLY\ MAINTENANCE\ FEE|WORLDWIDE\ ATM|US\ CORPORATION|VENMO|NORTH\ BAY\ CREDIT\ UNION|PAYPAL|ISLAMIC\ CENTER\ OF\ VENTURA\ COUNTY|CCD\ \-\ FINANCE\ CONTRACT\ PAYMENT\ 79590618\ E\ T)(?![A-Z0-9])'), '6. BANK CHARGES & FEES'),
    (re.compile(r'(?i)\b(?:HONOR|HEADQUARTERS\ LLC)\b'), '6. CONSULTANT & PROFESSIONAL SERVICES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ECHTRAI\ LLC|TWITCHTVDON|SIBANNA\ CONSULTANTS|JPS\ INC\.|EUNOIA\ CONSULTING\ GROUP\ LLC|DOUG\ NGO)(?![A-Z0-9])'), '6. CONSULTANT, LEGAL & PROFESSIONAL SERVICES'),
    (re.compile(r'(?i)\b(?:HISIG|STONEMARK|NEXT\ WAVE)\b'), '6. INSURANCE'),
    (re.compile(r'(?i)\b(?:LAW\ OFFICE\ OF\ CARLOS\ JATO)\b'), '6. LEGAL FEES'),
    (re.compile(r'(?i)\b(?:DCC|CA\ SECRETARY\ OF\ STATE|CAFRNCHISTXBRD|FRANCHISE\ TAX)\b'), '6. LICENSES & PERMITS'),
    (re.compile(r'(?i)\b(?:LB\ MEDIA\ GROUP\ LLC|KLAVIYO|MESA|GHOST\ MGMT|PLIVO|TWILIO|JOINTCOMMERCE|LHI|GOOGLE\ ADS|HAPPY\ CABBAGE|ALPINE\ IQ|SPARKS\ MARKETING|MESA\ OUTDOOR\ BILLB|DEEP\ ROOTS|ZENITH\ BILLBOARD|SPRINGBIG|DOPE\ MARKETING|SEMRUSH|MEDIAJEL|STEADY\ DEMAND)\b'), '6. MARKETING'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:COMCAST|T\-MOBILE|ATT|APPFOLIO|SPAGHETTI\ NETWORK\ INC|PRIMO\ WATER|FEDEX|COSTCO|TARGET|READYREFRESH|U\-HAUL|INC|GOOGLE\ FI|THE\ HOME\ DEPOT)(?![A-Z0-9])'), '6. OFFICE/GENERAL SUPPLIES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:SOUTHWEST|DOORDASH|MORGAN\ AT\ PROVOST\ SQUARE|UNITED|SPIRIT\ AIRLINES|MORGAN\ AT\ PROVOST|LAZ\ PKG\ OAKLAND|HOLIDAY\ INN|THE\ MORGAN|EARLYBRD|UPGBOARD|CANOPY\ JERSEY\ CITY|NYCDOT\ PARKING\ METERS|S\ FRIDGE|FARMER|ALAMO\ RENT\-A\-CAR|LAFAYETTE\ RES\ METER\ PARK)(?![A-Z0-9])'), '6. TRAVEL & MEALS EXPENSES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CHEVRON|SHELL|FASTRAK|ARCO|SPEEDWAY|LOVE|0807\ OUTSIDE|EXXON|ONFLEET|ENTERPRISE\ RENT\-A\-CAR|CA\ DMV|BUTTONWILLOW\ SINCLAIR|GAS\ 4\ LESS|PASS|E\-Z|DIXON\ GAS\ \&\ SHOP|MOMENTUM\ IOT|GAS\ \&\ FOOD|DMV|7\-ELEVEN)(?![A-Z0-9])'), '6. VEHICLE EXPENSES'),
    (re.compile(r'(?i)\b(?:INFINITY)\b'), '6. VEHICLE INSURANCE'),
    (re.compile(r'(?i)\b(?:SALES\ AND\ USE|EXCISE|CITY\ OF\ SACRAMENTO|CDTFA)\b'), '7. TAXES'),
]

# Which fields we concatenate to form the searchable text.
# Order matters; we include payee_vendor if already resolved by a previous rulebook.
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
      cf_account@<version>#<id>
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
      (value, rule_tag) where value is the resolved cf_account or "UNKNOWN".
      rule_tag is "cf_account@<version>#<rule_id>" or "" if unknown.
    """
    text = _concat_row(row)
    if not text:
        return (UNKNOWN, "")

    for i, rule in enumerate(_RULES, start=1):
        if len(rule) == 3:
            pat, cf_value, custom_id = rule  # type: ignore[misc]
        else:
            pat, cf_value = rule  # type: ignore[misc]
            custom_id = None

        if pat.search(text):
            return (cf_value, _rule_tag(i, custom_id))

    return (UNKNOWN, "")

def apply_rules_df(df) -> "pd.DataFrame":  # type: ignore
    """
    Vectorized convenience that produces 4 columns:
      cf_account, cf_account_rule_tag, cf_account_confidence, cf_account_source
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
    out["cf_account"] = values
    out["cf_account_rule_tag"] = tags
    out["cf_account_confidence"] = confs
    out["cf_account_source"] = srcs
    return out
