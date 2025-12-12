# src/rulebook/cf_account.py
from __future__ import annotations
from typing import Iterable, Tuple, List, Dict
import re
import pandas as pd

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
RULEBOOK_VERSION = "2025.12.13"   # <-- update when rules change
UNKNOWN = "UNKNOWN"

# ---------------------------------------------------------------------------
# Patterns: list of (compiled_regex, resolved_cf_account, optional_rule_id_label)
# Optionally provide a human-friendly id; if omitted the ordinal index is used.
# ---------------------------------------------------------------------------
_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    # CC Payment (explicit)
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bCHASE\s+CREDIT\s+CRD\b.*\bBANK_ACCOUNT_CC:[^|]*\bDAMA\s+7403\b'), '2. CC PAYMENT', 'new-chase-credit-crd-dama-7403'),

    # Licenses & Permits (EWB 3447 condition row)
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bEWB\b.*\bBANK_ACCOUNT_CC:[^|]*\bEWB\s+3447\b'), '6. LICENSES & PERMITS', 'new-ewb-ewb-3447'),

    # Distribution DMD via specific bank
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bCIRCLE\s+MUSKRAT\b.*\bBANK_ACCOUNT_CC:[^|]*\bEWB\s+8452\b'), '1. DISTRIBUTION DMD', 'new-circle-muskrat-ewb-8452'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bCURRENCY\s+AND\s+COIN\s+DEPOSITED\b.*\bBANK_ACCOUNT_CC:[^|]*\bEWB\s+8452\b'), '1. DISTRIBUTION DMD', 'new-currency-coin-ewb-8452'),

    # Transfer via bank
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bHAH\s+7\s+CA\s+LLC\b.*\bBANK_ACCOUNT_CC:[^|]*\bEWB\s+8452\b'), '2. TRANSFER', 'new-hah-7-ca-llc-ewb-8452'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bSMOAKLAND\s+WEED\s+DELIVERY\b.*\bBANK_ACCOUNT_CC:[^|]*\bEWB\s+8452\b'), '2. TRANSFER', 'new-smoakland-weed-delivery-ewb-8452'),

    # Payroll by bank
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bGUSTO\b.*\bBANK_ACCOUNT_CC:[^|]*\bDAMA\s+5597\b'), '4. PAYROLL SOCAL', 'new-gusto-dama-5597'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bGUSTO\b.*\bBANK_ACCOUNT_CC:[^|]*\bKP\s+6852\b'), '4. PAYROLL HAH', 'new-gusto-kp-6852'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bGUSTO\b.*\bBANK_ACCOUNT_CC:[^|]*\bCC\s+9551\b|\bBANK_ACCOUNT_CC:[^|]*\bNBCU\s+2211\b'), '4. PAYROLL SUB', 'new-gusto-sub-2211'),

    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bPAYNW\b.*\bBANK_ACCOUNT_CC:[^|]*\bEWB\s+8452\b'), '4. PAYROLL DMD', 'new-paynw-ewb-8452'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bPAYNW\b.*\bBANK_ACCOUNT_CC:[^|]*\bKP\s+6852\b'), '4. PAYROLL HAH', 'new-paynw-kp-6852'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bPAYNW\b.*\bBANK_ACCOUNT_CC:[^|]*\bNBCU\s+2035\b'), '4. PAYROLL DMD', 'new-paynw-nbcu-2035'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bPAYNW\b.*\bBANK_ACCOUNT_CC:[^|]*\bNBCU\s+SWD\b'), '4. PAYROLL DMD', 'new-paynw-nbcu-swd'),

    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bWURK\b.*\bBANK_ACCOUNT_CC:[^|]*\bDAMA\s+7403\b'), '4. PAYROLL DMD', 'new-wurk-dama-7403'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bWURK\b.*\bBANK_ACCOUNT_CC:[^|]*\bNBCU\s+SWD\b'), '4. PAYROLL DMD', 'new-wurk-nbcu-swd'),

    # NY Expense (NEW bucket)
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bCHANG\s+YI\b.*\bBANK_ACCOUNT_CC:[^|]*\bDAMA\s+7403\b'), '8. NY EXPENSE', 'new-chang-yi-dama-7403'),
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bMARCO\s+FEI\b'), '8. NY EXPENSE', 'new-marco-fei-ny-expense'),

    # General rules
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
_SOURCE_COLS = [
    "payee_vendor", 'bank_account_cc', 'amount',
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
        v = row.get(c, "") or ""
        v = str(v)
        if v.strip():
            parts.append(f"{c.upper()}: {v}")
    return _normalize_text(" | ".join(parts))

def _rule_tag(i: int, custom: str | None) -> str:
    """
    Build a stable provenance tag:
      cf_account@<version>#<id>
    where <id> is either an explicit label (custom) or the 1-based index.
    """
    rid = custom if (custom and custom.strip()) else str(i)
    return f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rid}"

_POSTPROCESS_SOURCE = "postprocess"

def postprocess(df: "pd.DataFrame") -> "pd.DataFrame":  # type: ignore
    if df is None or df.empty:
        return df

    need = {"payee_vendor", "bank_account_cc", "amount", "cf_account"}
    if not need.issubset(df.columns):
        return df

    out = df.copy()
    amt = pd.to_numeric(out["amount"], errors="coerce")
    payee = out["payee_vendor"].fillna("").astype(str).str.upper()
    acct = out["bank_account_cc"].fillna("").astype(str).str.upper()

    def stamp(mask, value, rule_id: str):
        if not mask.any():
            return
        out.loc[mask, "cf_account"] = value
        if "cf_account_rule_tag" in out.columns:
            out.loc[mask, "cf_account_rule_tag"] = f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rule_id}"
        if "cf_account_source" in out.columns:
            out.loc[mask, "cf_account_source"] = _POSTPROCESS_SOURCE
        if "cf_account_confidence" in out.columns:
            out.loc[mask, "cf_account_confidence"] = 1.0

    # --- Aeropay by bank + sign ---
    aer = payee.eq("AEROPAY")

    stamp(aer & acct.eq("DAMA 5597") & (amt > 0), "1. RETAIL SOCAL", "pp-aeropay-dama-5597-pos")
    stamp(aer & acct.eq("DAMA 5597") & (amt < 0), "6. BANK CHARGES & FEES", "pp-aeropay-dama-5597-neg")

    stamp(aer & acct.eq("KP 6852") & (amt > 0), "1. RETAIL HAH", "pp-aeropay-kp-6852-pos")
    stamp(aer & acct.eq("KP 6852") & (amt < 0), "6. BANK CHARGES & FEES", "pp-aeropay-kp-6852-neg")

    stamp(aer & acct.eq("NBCU 2035") & (amt > 0), "1. RETAIL DMD", "pp-aeropay-nbcu-2035-pos")
    stamp(aer & acct.eq("NBCU 2035") & (amt < 0), "6. BANK CHARGES & FEES", "pp-aeropay-nbcu-2035-neg")

    stamp(aer & acct.eq("NBCU SWD") & (amt > 0), "1. RETAIL DMD", "pp-aeropay-nbcu-swd-pos")
    stamp(aer & acct.eq("NBCU SWD") & (amt < 0), "6. BANK CHARGES & FEES", "pp-aeropay-nbcu-swd-neg")

    # --- Armored Car sign ---
    arm = payee.eq("ARMORED CAR")
    stamp(arm & (amt > 0), "1. RETAIL DMD", "pp-armored-car-pos")
    stamp(arm & (amt < 0), "6. BANK CHARGES & FEES", "pp-armored-car-neg")

    # --- Bud Technology sign ---
    bud = payee.eq("BUD TECHNOLOGY")
    stamp(bud & (amt > 0), "1. DISTRIBUTION DMD", "pp-bud-technology-pos")
    stamp(bud & (amt < 0), "5. OTHER COGS", "pp-bud-technology-neg")

    # --- OSS sign ---
    oss = payee.eq("OSS")
    stamp(oss & (amt > 0), "1. RETAIL HAH", "pp-oss-pos")
    stamp(oss & (amt < 0), "6. BANK CHARGES & FEES", "pp-oss-neg")

    # --- Odoo B2B + EWB 3447 + positive ---
    odoo = payee.eq("ODOO B2B") & acct.eq("EWB 3447") & (amt > 0)
    stamp(odoo, "1. DISTRIBUTION DMD", "pp-odoo-b2b-ewb-3447-pos")

    # --- Blaze exact ±1050 -> Apps & Software ---
    blaze = payee.eq("BLAZE") & (amt.abs() == 1050.00)
    stamp(blaze, "6. APPS & SOFTWARE", "pp-blaze-1050")

    # --- Xeven Solutions exact ±3200 -> Marketing ---
    xev = payee.eq("XEVEN SOLUTIONS") & (amt.abs() == 3200.00)
    stamp(xev, "6. MARKETING", "pp-xeven-3200")

    return out

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

def apply_rules_df(df) -> "pd.DataFrame":
    """
    Vectorized convenience that produces 4 columns:
      cf_account, cf_account_rule_tag, cf_account_confidence, cf_account_source
    """

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
    return postprocess(out)
