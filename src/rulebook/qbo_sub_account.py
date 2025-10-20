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
    (re.compile(r'(?i)(?<![A-Z0-9])PLIVOCOM(?![A-Z0-9])'), 'Smoakland Media Expense:Advertising and Promotion'),
    (re.compile(r'(?i)(?<![A-Z0-9])PLIVO(?![A-Z0-9])'), 'Smoakland Media Expense:Advertising and Promotion'),
    (re.compile(r'(?i)(?<![A-Z0-9])EPAYMENT(?![A-Z0-9])'), 'CC 71000'),
    (re.compile(r'(?i)(?<![A-Z0-9])ECHTRAI(?![A-Z0-9])'), 'TPH786 Expenses:Consultant Fees'),
    (re.compile(r'(?i)(?<![A-Z0-9])CITI(?![A-Z0-9])'), 'CC 71000'),
    (re.compile(r'(?i)(?<![A-Z0-9])KONCAUSEWAY(?![A-Z0-9])'), 'Due from SUB'),
    (re.compile(r'(?i)(?<![A-Z0-9])321177573(?![A-Z0-9])'), 'Sublime'),
    (re.compile(r'(?i)(?<![A-Z0-9])PASSNY(?![A-Z0-9])'), 'Due from TPH'),
    (re.compile(r'(?i)(?<![A-Z0-9])PASS(?![A-Z0-9])'), 'Due from TPH'),
    (re.compile(r'(?i)(?<![A-Z0-9])REBILL(?![A-Z0-9])'), 'Due from TPH'),
    (re.compile(r'(?i)(?<![A-Z0-9])SQJERSEY(?![A-Z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Z0-9])60638(?![A-Z0-9])'), 'Vehicle Compliance'),
    (re.compile(r'(?i)(?<![A-Z0-9])HOLDING(?![A-Z0-9])'), 'TPH'),
    (re.compile(r'(?i)(?<![A-Z0-9])MEDIAJEL(?![A-Z0-9])'), 'Smoakland Media Expense:Advertising and Promotion'),
    (re.compile(r'(?i)(?<![A-Z0-9])83666(?![A-Z0-9])'), 'TPH786 Expenses:Insurance'),
    (re.compile(r'(?i)(?<![A-Z0-9])JPS(?![A-Z0-9])'), 'TPH786 Expenses:Legal & Professional'),
    (re.compile(r'(?i)(?<![A-Z0-9])SENDGRID(?![A-Z0-9])'), 'Smoakland Media Expense:Advertising and Promotion'),
    (re.compile(r'(?i)(?<![A-Z0-9])1995(?![A-Z0-9])'), 'Smoakland Media Expense:Advertising and Promotion'),
    (re.compile(r'(?i)(?<![A-Z0-9])PORTER(?![A-Z0-9])'), 'Due from SUB'),
    (re.compile(r'(?i)(?<![A-Z0-9])ELHSBCHKHHHKH(?![A-Z0-9])'), 'Due from SUB'),
    (re.compile(r'(?i)(?<![A-Z0-9])JATO(?![A-Z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Z0-9])EVENT(?![A-Z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Z0-9])INTERESTHUMAN(?![A-Z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Z0-9])ADMINPSADMINIST(?![A-Z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Z0-9])56296(?![A-Z0-9])'), 'TPH786 Expenses:Insurance'),
    (re.compile(r'(?i)(?<![A-Z0-9])39735(?![A-Z0-9])'), 'Smoakland Media Expense:Telephone Expense'),
    (re.compile(r'(?i)(?<![A-Z0-9])EUNOIA(?![A-Z0-9])'), 'TPH786 Expenses:Consultant Fees'),
    (re.compile(r'(?i)(?<![A-Z0-9])DELIVERMDTRANSFER(?![A-Z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Z0-9])FINANCIALS(?![A-Z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Z0-9])HONG(?![A-Z0-9])'), 'Due from SUB'),
    (re.compile(r'(?i)(?<![A-Z0-9])7999(?![A-Z0-9])'), 'Smoakland Media Expense:Software & Apps'),
    (re.compile(r'(?i)(?<![A-Z0-9])AMEX(?![A-Z0-9])'), 'CC 71000'),
    (re.compile(r'(?i)(?<![A-Z0-9])ROOTS(?![A-Z0-9])'), 'Smoakland Media Expense:Advertising and Promotion'),
    (re.compile(r'(?i)(?<![A-Z0-9])PICKUP(?![A-Z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Z0-9])CONFIRMED(?![A-Z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Z0-9])7975(?![A-Z0-9])'), 'Smoakland Media Expense:Software & Apps'),
    (re.compile(r'(?i)(?<![A-Z0-9])SAMSARA(?![A-Z0-9])'), 'Vehicle Compliance'),
    (re.compile(r'(?i)(?<![A-Z0-9])TIRE(?![A-Z0-9])'), 'DeliverMD Expenses:Delivery Vehicle Expenses:Repairs and Maintenance'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])CC\\ 8202(?![A-Za-z0-9])'), 'CC 8202'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])CC\\ 9551(?![A-Za-z0-9])'), 'CC 9551'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Cannabis\\ Sales(?![A-Za-z0-9])'), 'DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Cost\\ of\\ Inventory(?![A-Za-z0-9])'), 'Sublime'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Cost\\ of\\ Production:Lab\\ Testing(?![A-Za-z0-9])'), 'Due from Sublime'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Cost\\ of\\ Production:Packaging(?![A-Za-z0-9])'), 'Due from SUB'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Cost\\ of\\ Production:Production\\ Supplies(?![A-Za-z0-9])'), 'Due From Sublime'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Dama\\ Financial\\ 2370(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Dama\\ Financials(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])DeliverMD\\ Expenses:Delivery\\ Vehicle\\ Expenses:Other\\ Expenses(?![A-Za-z0-9])'), 'Due from DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Delivery\\ COGS:Apps\\ \\&\\ Software(?![A-Za-z0-9])'), 'Due from DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Delivery\\ COGS:Software\\ \\&\\ Apps(?![A-Za-z0-9])'), 'Due from HAH 7 LLC'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Delivery\\ COGS:Vehicle\\ Compliance(?![A-Za-z0-9])'), 'Due From DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Distribution\\ Revenue(?![A-Za-z0-9])'), 'HAH'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ From\\ BB\\ \\&\\ SE(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ From\\ DMD(?![A-Za-z0-9])'), 'Distribution COGS:Packaging'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ From\\ HAH\\ 7\\ LLC(?![A-Za-z0-9])'), 'Security Expenses'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ From\\ SoCal\\ \\(TPH\\)(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ From\\ Sublime(?![A-Za-z0-9])'), 'CC 9551'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ From\\ TPH786(?![A-Za-z0-9])'), 'Smoakland Media Expense:Advertising and Promotion'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ DMD(?![A-Za-z0-9])'), 'Vehicle Expenses:Fuel'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ Distro\\ for\\ Inventory(?![A-Za-z0-9])'), 'Due from DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ HAH(?![A-Za-z0-9])'), 'Vehicle Expenses:Fuel'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ HAH\\ 7\\ LLC(?![A-Za-z0-9])'), 'Vehicle Expenses:Fuel'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ SUB(?![A-Za-z0-9])'), 'Software & Apps'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ Socal(?![A-Za-z0-9])'), 'Due from Socal'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ Sublime(?![A-Za-z0-9])'), 'CC 9551'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ TPH(?![A-Za-z0-9])'), 'Facility Costs:Utilities'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ TPH\\ for\\ Inventory(?![A-Za-z0-9])'), 'Due from TPH'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ from\\ TPH786(?![A-Za-z0-9])'), 'TPH786 Expenses:Insurance'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ to\\ DMD(?![A-Za-z0-9])'), 'NBCU 2035'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ to\\ DeliverMD(?![A-Za-z0-9])'), 'Distribution Revenue'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ to\\ HAH(?![A-Za-z0-9])'), 'KP 6852'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ to\\ NY\\ for\\ Inventory(?![A-Za-z0-9])'), 'Due to NY'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ to\\ SUB(?![A-Za-z0-9])'), 'NBCU 2211'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ to\\ TPH\\ for\\ Inventory(?![A-Za-z0-9])'), 'Due to TPH'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Due\\ to\\ TPH786(?![A-Za-z0-9])'), 'CC 71000'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])East\\ West\\ Bank\\ 3447(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Facility\\ Costs:Licenses\\ \\&\\ Permits(?![A-Za-z0-9])'), 'Due from HAH 7 LLC'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Facility\\ Costs:Security\\ Expense(?![A-Za-z0-9])'), 'Due from HAH 7 LLC'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Facility\\ Costs:Utilities(?![A-Za-z0-9])'), 'Due from HAH 7 LLC'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Funding(?![A-Za-z0-9])'), 'Due to TPH'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])GreenBax\\ 0074(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Greenbax\\ 0073(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Greenbax\\ 01875(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Intercompany:Due\\ From\\ TPH786(?![A-Za-z0-9])'), 'Smoakland Media Expense:Event Expense'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Intercompany:Due\\ to\\ DMD(?![A-Za-z0-9])'), 'NBCU 2035'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Management\\ Fee\\ Expense(?![A-Za-z0-9])'), 'TPH'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Management\\ Fee\\ Revenue(?![A-Za-z0-9])'), 'DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])NBCU\\ 1791(?![A-Za-z0-9])'), 'Transfer'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])NBCU\\ 2211(?![A-Za-z0-9])'), 'NBCU 2211'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])North\\ Bay\\ Credit\\ Union\\ Checking\\ 2035(?![A-Za-z0-9])'), 'NBCU 2035'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Overhead\\ Costs:Rent\\ Expense(?![A-Za-z0-9])'), 'Due From Sublime'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Overhead\\ Costs:Security\\ Expense(?![A-Za-z0-9])'), 'Due from Sublime'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Overhead\\ Costs:Utilities\\ Expense:Waste\\ Disposal(?![A-Za-z0-9])'), 'Due from SUB'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Cannabis\\ Sales(?![A-Za-z0-9])'), 'HAH'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Distribution\\ Expenses:Distribution\\ Vehicle\\ Expenses(?![A-Za-z0-9])'), 'Due from DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Distribution\\ Expenses:Fuel\\ \\&\\ Oil(?![A-Za-z0-9])'), 'Due From DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Distribution\\ Expenses:Repairs\\ \\&\\ Maintenance(?![A-Za-z0-9])'), 'Due From DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Distribution\\ Expenses:Vehicle\\ Insurance(?![A-Za-z0-9])'), 'Due from DMD'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Media\\ Expense:Advertising\\ and\\ Promotion(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Media\\ Expense:Event\\ Expense(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Media\\ Expense:Internet\\ \\&\\ Telephone\\ Expense(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ Media\\ Expense:Software\\ \\&\\ Apps(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ New\\ York\\ Expense:Licensing\\ \\&\\ Permit(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ New\\ York\\ Expense:Utilities(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ New\\ York\\ Expense:Vehicle\\ Expense(?![A-Za-z0-9])'), 'Due from TPH'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ New\\ York\\ Expense:Vehicle\\ Insurance(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ SoCal\\ Expense:Bank\\ Charges\\ \\&\\ Fees(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ SoCal\\ Expense:City\\ Tax(?![A-Za-z0-9])'), 'Due From SoCal'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ SoCal\\ Expense:Rent\\ Expense(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ SoCal\\ Expense:Security\\ Expenses(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ SoCal\\ Expense:Software\\ \\&\\ Apps(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Smoakland\\ SoCal\\ Expense:Utilities(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Bank\\ Charges(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Consultant\\ and\\ Professional\\ Fees(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Insurance(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Legal\\ Fees(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Meals\\ /\\ Lunches(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Office\\ Expenses(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Repairs\\ and\\ Maintenance(?![A-Za-z0-9])'), 'Due from TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])TPH786\\ Expenses:Travel\\ Expense(?![A-Za-z0-9])'), 'Due From TPH786'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Taxes:CDTFA\\ Tax(?![A-Za-z0-9])'), 'Excise'),
    (re.compile(r'(?i)(?<![A-Za-z0-9])Taxes:City\\ of\\ Sacramento\\ Tax(?![A-Za-z0-9])'), 'Due from HAH 7 LLC'),
]

_SOURCE_COLS: List[str] = [
    "bank_account", "subentity", "bank_cc_num",
    "payee_vendor", "cf_account", "qbo_account", "entity_qbo",
    "description", "extended_description",
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
