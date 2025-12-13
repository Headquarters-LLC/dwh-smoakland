# src/rulebook/qbo_account.py
from __future__ import annotations
from typing import Tuple, List, Dict
import re
import pandas as pd

"""
qbo_account rulebook
--------------------
- Contract: infer(row) -> (value, rule_tag)
  * value:    resolved QBO Account (str) or "" if unknown
  * rule_tag: provenance "qbo_account@<version>#<rule_id>" or "" if unknown

"""

RULEBOOK_NAME = "qbo_account"
RULEBOOK_VERSION = "2025.12.13"  # bump: add scoped rules + CSV-prepended rules
UNKNOWN = ""                         # <-- fallback is blank for unknowns

# ---------------------------------------------------------------------------
# Patterns: list of (compiled_regex, resolved_value, optional_rule_id_label)
# If no label is provided, the ordinal index (1-based) is used for the tag.
# ---------------------------------------------------------------------------
_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    # NEW: TPH scoped
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bTPH\b.*\bPAYEE_VENDOR:[^|]*\bAD\s+ASTRA\s+LAW\s+GROUP\b'),
     'TPH786 Expenses:Legal Fees', 'new-tph-ad-astra-legal'),

    # Apps & Software -> Smoakland Media Expense:Software & Apps
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bTPH\b.*\bPAYEE_VENDOR:[^|]*\b(?:ADOBE|AIRCALL|AMAZON\s+WEB\s+SERVICES|APPFOLIO(?:,\s*INC)?|BROWSERSTACK|CANVA|JOINHOMEBASE|QUICKBOOKS|SPECTRUM)\b'),
     'Smoakland Media Expense:Software & Apps', 'new-tph-software-apps'),

    # Internet & Telephone (Administration bucket, etc.)
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bTPH\b.*\bPAYEE_VENDOR:[^|]*\b(?:ATT|COMCAST|FRONTIER\s+COMM\s+CORP\s+WEB|T\-MOBILE)\b'),
     'Smoakland Media Expense:Internet & Telephone Expense', 'new-tph-internet-telephone'),

    # Advertising & Promotion (Marketing)
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bTPH\b.*\bPAYEE_VENDOR:[^|]*\b(?:GHOST\s+MGMT|KLAVIYO|SEMRUSH|SPRINGBIG|STEADY\s+DEMAND|XEVEN\s+SOLUTIONS)\b'),
     'Smoakland Media Expense:Advertising and Promotion', 'new-tph-advertising'),

    # Office Expenses (Amazon CC 7639)
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bTPH\b.*\bPAYEE_VENDOR:[^|]*\bAMAZON\b.*\b(?:BANK_ACCOUNT_CC|BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bCC\s*7639\b'),
     'TPH786 Expenses:Office Expenses', 'new-tph-amazon-cc7639-office'),

    # -----------------------------
    # NEW: SSC scoped (Distribution / Sublime)
    # -----------------------------
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\b(?:ALAMO\s+TOLL|AVIS\s+FEE|CA\s+DMV|ENTERPRISE\s+RENT\-A\-CAR|ERACTOLL|ORINDA)\b'),
     'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'new-ssc-dist-vehicle'),

    (re.compile(r"(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\b(?:ARCO|CHEVRON|EXXON|GAWFCO\s+WESTLEY|LOVE'S|PILOT|UNION|VALERO)\b"),
     'Smoakland Distribution Expenses:Fuel & Oil', 'new-ssc-fuel-oil'),

    # Amazon CC 9551 -> Production Supplies
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\bAMAZON\b.*\b(?:BANK_ACCOUNT_CC|BANK_ACCOUNT|BANK_CC_NUM):[^|]*\bCC\s*9551\b'),
     'Cost of Production:Production Supplies', 'new-ssc-amazon-cc9551-prod-supplies'),

    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\bCHEFSTORE\b'),
     'Cost of Production:Production Supplies', 'new-ssc-chefstore-prod-supplies'),

    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\bCINTAS\s+CORP\b'),
     'Cost of Production:Uniforms', 'new-ssc-cintas-uniforms'),

    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\b(?:HARRENS\s+LAB\s+INC\.?|MCR\s+LABS\s+\-\s+NY)\b'),
     'Cost of Production:Lab Testing', 'new-ssc-lab-testing'),

    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\b(?:HONGKONG\s+PORTER\s+ELECTRONICS\s+LIMITED|ULINE)\b'),
     'Cost of Production:Packaging', 'new-ssc-packaging'),

    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\bMEDIWASTE\s+DISPOSAL\s+LLC\b'),
     'Overhead Costs:Utilities Expense:Waste Disposal', 'new-ssc-mediwaste'),

    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bSSC\b.*\bPAYEE_VENDOR:[^|]*\b(?:CIRCLE\s+MUSKRAT|CURRENCY\s+AND\s+COIN\s+DEPOSITED|BRYANT\s+AND\s+GILBE|OTC\s+WEHO)\b'),
     'Distribution Revenue', 'new-ssc-distribution-revenue'),

    # NY entity: NY Distribution Revenue group
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bNY\b.*\bPAYEE_VENDOR:[^|]*\b(?:BLAZE\s+420\s+LLC\s+27|CAFFIEND\s+THE\s+BUSHWICK\s+DISPENSARY|CASE\s+MANAGEMENT\s+INC|FRESHLY\s+BAKED\s+NYC|GUARDIAN\s+WELLNESS|HAPPY\s+DAYS\s+DISPENSARY\s+INC|NSEW\s+TRADING\s+COMPANY\s+LLC|NUCLEUS\s+DISPENSARY|THE\s+PEOPLES\s+JOINT|TWENTY8GRAMZ)\b'),
     'NY Distribution Revenue', 'new-ny-distribution-revenue'),

    # NY expense specific
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bNY\b.*\bPAYEE_VENDOR:[^|]*\bCHANG\s+YI\b'),
     'Smoakland New York Expense:Commissions', 'new-ny-chang-yi-commissions'),
    (re.compile(r'(?i)\bENTITY_QBO:[^|]*\bNY\b.*\bPAYEE_VENDOR:[^|]*\bMARCO\s+FEI\b'),
     'Smoakland New York Expense:Rent Expense', 'new-ny-marco-fei-rent'),

    #Legacy rules
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAD\s+ASTRA\s+LAW\s+GROUP\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bAD\s+ASTRA\s+LAW\s+GROUP\b)'), 'TPH786 Expenses:Legal Fees', 'csv-1-AD-ASTRA-LAW-GROUP'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bADOBE\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bADOBE\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-2-ADOBE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAIRCALL\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bAIRCALL\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-3-AIRCALL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAMAZON\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bAMAZON\b)'), 'TPH786 Expenses:Office Expenses', 'csv-4-AMAZON'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAMAZON\s+WEB\s+SERVICES\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bAMAZON\s+WEB\s+SERVICES\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-5-AMAZON-WEB-SERVICES'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bATT\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bATT\b)'), 'Smoakland Media Expense:Internet & Telephone Expense', 'csv-6-ATT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBROWSERSTACK\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bBROWSERSTACK\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-7-BROWSERSTACK'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCANVA\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bCANVA\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-8-CANVA'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCLTV8\s+LLC\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bCLTV8\s+LLC\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-9-CLTV8-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCOMCAST\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bCOMCAST\b)'), 'Smoakland Media Expense:Internet & Telephone Expense', 'csv-10-COMCAST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+CRAVESUBS\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+CRAVESUBS\b)'), 'TPH786 Expenses:Meals / Lunches', 'csv-11-DOORDASH-CRAVESUBS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+PHOHUYNHH\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+PHOHUYNHH\b)'), 'TPH786 Expenses:Meals / Lunches', 'csv-12-DOORDASH-PHOHUYNHH'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+SUMMERKIT\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+SUMMERKIT\b)'), 'TPH786 Expenses:Meals / Lunches', 'csv-13-DOORDASH-SUMMERKIT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+TARGET\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bDOORDASH\s+TARGET\b)'), 'TPH786 Expenses:Meals / Lunches', 'csv-14-DOORDASH-TARGET'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEASTWEST\s+BANK\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bEASTWEST\s+BANK\b)'), 'TPH786 Expenses:Bank Charges', 'csv-15-EASTWEST-BANK'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bFRONT\s+GROWTH\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bFRONT\s+GROWTH\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-16-FRONT-GROWTH'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bFRONTIER\s+COMM\s+CORP\s+WEB\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bFRONTIER\s+COMM\s+CORP\s+WEB\b)'), 'Smoakland Media Expense:Internet & Telephone Expense', 'csv-17-FRONTIER-COMM-CORP-WEB'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGHOST\s+MGMT\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bGHOST\s+MGMT\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-18-GHOST-MGMT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGITHUB\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bGITHUB\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-19-GITHUB'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGOOGLE\s+CLOUD\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bGOOGLE\s+CLOUD\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-20-GOOGLE-CLOUD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGOOGLE\s+GSUITE\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bGOOGLE\s+GSUITE\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-21-GOOGLE-GSUITE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHARBOR\s+HR\s+LLC\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bHARBOR\s+HR\s+LLC\b)'), 'TPH786 Expenses:Consultant and Professional Fees', 'csv-22-HARBOR-HR-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHEADQUARTERS\s+LLC\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bHEADQUARTERS\s+LLC\b)'), 'TPH786 Expenses:Consultant and Professional Fees', 'csv-23-HEADQUARTERS-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHISIG\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bHISIG\b)'), 'TPH786 Expenses:Insurance', 'csv-24-HISIG'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHONOR\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bHONOR\b)'), 'TPH786 Expenses:Consultant and Professional Fees', 'csv-25-HONOR'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHUMAN\s+INTEREST\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bHUMAN\s+INTEREST\b)'), 'TPH786 Expenses:Insurance', 'csv-26-HUMAN-INTEREST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHUMANA,\s+INC\.\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bHUMANA,\s+INC\.\b)'), 'TPH786 Expenses:Insurance', 'csv-27-HUMANA-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bJOINHOMEBASE\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bJOINHOMEBASE\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-28-JOINHOMEBASE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bKAISER\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bKAISER\b)'), 'TPH786 Expenses:Insurance', 'csv-29-KAISER'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bKLAVIYO\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bKLAVIYO\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-30-KLAVIYO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bLAW\s+OFFICE\s+OF\s+CARLOS\s+JATO\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bLAW\s+OFFICE\s+OF\s+CARLOS\s+JATO\b)'), 'TPH786 Expenses:Legal Fees', 'csv-31-LAW-OFFICE-OF-CARLOS-JATO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bLEGALZOOM\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bLEGALZOOM\b)'), 'TPH786 Expenses:Legal Fees', 'csv-32-LEGALZOOM'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMATRIX\s+TRUST\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bMATRIX\s+TRUST\b)'), 'TPH786 Expenses:Insurance', 'csv-33-MATRIX-TRUST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMESA\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bMESA\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-34-MESA'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMICROSOFT\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bMICROSOFT\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-35-MICROSOFT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMORGAN\s+AT\s+PROVOST\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bMORGAN\s+AT\s+PROVOST\b)'), 'TPH786 Expenses:Travel Expense', 'csv-36-MORGAN-AT-PROVOST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMORGAN\s+AT\s+PROVOST\s+SQUARE\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bMORGAN\s+AT\s+PROVOST\s+SQUARE\b)'), 'TPH786 Expenses:Travel Expense', 'csv-37-MORGAN-AT-PROVOST-SQUARE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPARKINGCENTRAL\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bPARKINGCENTRAL\b)'), 'TPH786 Expenses:Travel Expense', 'csv-38-PARKINGCENTRAL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPS\s+ADMINISTRATORS\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bPS\s+ADMINISTRATORS\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-39-PS-ADMINISTRATORS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bQUICKBOOKS\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bQUICKBOOKS\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-40-QUICKBOOKS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSEMRUSH\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSEMRUSH\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-41-SEMRUSH'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSMOAKLAND\s+NY\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSMOAKLAND\s+NY\b)'), 'Due to NY for Inventory', 'csv-42-SMOAKLAND-NY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSOUTHWEST\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSOUTHWEST\b)'), 'TPH786 Expenses:Travel Expense', 'csv-43-SOUTHWEST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSPECTRUM\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSPECTRUM\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-44-SPECTRUM'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSPRINGBIG\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSPRINGBIG\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-45-SPRINGBIG'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSTAR\s+COPY\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSTAR\s+COPY\b)'), 'TPH786 Expenses:Office Expenses', 'csv-46-STAR-COPY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSTEADY\s+DEMAND\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSTEADY\s+DEMAND\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-47-STEADY-DEMAND'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSTOCKIST\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bSTOCKIST\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-48-STOCKIST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bT\-MOBILE\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bT\-MOBILE\b)'), 'Smoakland Media Expense:Internet & Telephone Expense', 'csv-49-T-MOBILE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bTHE\s+PUREST\s+HEART\s+786\s+INC\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bTHE\s+PUREST\s+HEART\s+786\s+INC\b)'), 'Due from TPH for Inventory', 'csv-50-THE-PUREST-HEART-786-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bUBER\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bUBER\b)'), 'TPH786 Expenses:Travel Expense', 'csv-51-UBER'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bUNITED\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bUNITED\b)'), 'TPH786 Expenses:Travel Expense', 'csv-52-UNITED'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bUPRINTING\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bUPRINTING\b)'), 'TPH786 Expenses:Travel Expense', 'csv-53-UPRINTING'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bUS\s+CORPORATION\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bUS\s+CORPORATION\b)'), 'TPH786 Expenses:Bank Charges', 'csv-54-US-CORPORATION'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bVERCEL\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bVERCEL\b)'), 'Smoakland Media Expense:Software & Apps', 'csv-55-VERCEL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bWHOP\s+EUPHORIX\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bWHOP\s+EUPHORIX\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-56-WHOP-EUPHORIX'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bWILINE\s+NETWORKS\s+INC\.\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bWILINE\s+NETWORKS\s+INC\.\b)'), 'Smoakland Media Expense:Internet & Telephone Expense', 'csv-57-WILINE-NETWORKS-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bWYKOWSKI\s+\&\s+WOOD\s+LLP\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bWYKOWSKI\s+\&\s+WOOD\s+LLP\b)'), 'TPH786 Expenses:Legal Fees', 'csv-58-WYKOWSKI-WOOD-LLP'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bXEVEN\s+SOLUTIONS\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bXEVEN\s+SOLUTIONS\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-59-XEVEN-SOLUTIONS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bZENITH\s+BILLBOARD\b.*?\bENTITY_QBO:[^|]*\bTPH\b|\bENTITY_QBO:[^|]*\bTPH\b.*?\bPAYEE_VENDOR:[^|]*\bZENITH\s+BILLBOARD\b)'), 'Smoakland Media Expense:Advertising and Promotion', 'csv-61-ZENITH-BILLBOARD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAEROPAY\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bAEROPAY\b)'), 'Bank/Armor Fees', 'csv-62-AEROPAY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBAY\s+ALARM\s+COMPANY\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bBAY\s+ALARM\s+COMPANY\b)'), 'Facility Costs:Security Expense', 'csv-64-BAY-ALARM-COMPANY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBLAZE\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bBLAZE\b)'), 'Software & Apps', 'csv-65-BLAZE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCDTFA\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bCDTFA\b)'), 'CDTFA Taxes', 'csv-66-CDTFA'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+SACRAMENTO\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+SACRAMENTO\b)'), 'City of Sacramento Taxes', 'csv-67-CITY-OF-SACRAMENTO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDCC\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bDCC\b)'), 'Facility Costs:Licenses & Permits', 'csv-68-DCC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEWB\s+3439\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bEWB\s+3439\b)'), 'Management Fee Expense', 'csv-69-EWB-3439'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEWB\s+8452\s+SSC\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bEWB\s+8452\s+SSC\b)'), 'Cost of Inventory', 'csv-70-EWB-8452-SSC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGUSTO\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bGUSTO\b)'), 'Direct Labor Cost', 'csv-71-GUSTO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHAH\s+7\s+CA\s+LLC\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bHAH\s+7\s+CA\s+LLC\b)'), 'Direct Labor Cost', 'csv-72-HAH-7-CA-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHUMAN\s+INTEREST\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bHUMAN\s+INTEREST\b)'), 'Direct Labor Cost', 'csv-73-HUMAN-INTEREST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bINTELLIWORX\s+PH\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bINTELLIWORX\s+PH\b)'), 'DeliverMD Expenses:Consultant and Professional Fees', 'csv-74-INTELLIWORX-PH'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bKEYPOINT\s+CREDIT\s+UNION\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bKEYPOINT\s+CREDIT\s+UNION\b)'), 'Bank/Armor Fees', 'csv-75-KEYPOINT-CREDIT-UNION'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bONFLEET\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bONFLEET\b)'), 'Delivery COGS:Vehicle Compliance', 'csv-76-ONFLEET'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bOSS\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bOSS\b)'), 'Bank/Armor Fees', 'csv-77-OSS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPAYNW\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bPAYNW\b)'), 'Direct Labor Cost', 'csv-79-PAYNW'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSACRAMENTO\s+MUNICIPAL\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bSACRAMENTO\s+MUNICIPAL\b)'), 'Facility Costs:Utilities', 'csv-80-SACRAMENTO-MUNICIPAL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSACRAMENTO\s+RENT\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bSACRAMENTO\s+RENT\b)'), 'Facility Costs:Rent Expense', 'csv-81-SACRAMENTO-RENT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSTRONGHOLD\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bSTRONGHOLD\b)'), 'Delivery Revenue:Cannabis', 'csv-82-STRONGHOLD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bWASTE\s+MGMT\b.*?\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b|\bENTITY_QBO:[^|]*\bHAH\s+7\s+CA\b.*?\bPAYEE_VENDOR:[^|]*\bWASTE\s+MGMT\b)'), 'Facility Costs:Utilities', 'csv-83-WASTE-MGMT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\b610\s+MARKET\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\b610\s+MARKET\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-84-610-MARKET'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\b7\-ELEVEN\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\b7\-ELEVEN\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-85-7-ELEVEN'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bA\&A\s+GAS\s+\&\s+FOOD\s+MART\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bA\&A\s+GAS\s+\&\s+FOOD\s+MART\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-86-A-A-GAS-FOOD-MART'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bALAMO\s+RENT\-A\-CAR\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bALAMO\s+RENT\-A\-CAR\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-87-ALAMO-RENT-A-CAR'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bALAMO\s+TOLL\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bALAMO\s+TOLL\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-88-ALAMO-TOLL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bALIBABA\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bALIBABA\b)'), 'Cost of Production:Packaging', 'csv-89-ALIBABA'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAMAZON\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bAMAZON\b)'), 'Cost of Production:Production Supplies', 'csv-90-AMAZON'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bARCO\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bARCO\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-91-ARCO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAREA\s+29\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bAREA\s+29\s+LLC\b)'), 'Distribution Revenue', 'csv-92-AREA-29-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAVIS\s+FEE\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bAVIS\s+FEE\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-93-AVIS-FEE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBMCV\s+WHOLESALE\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bBMCV\s+WHOLESALE\s+LLC\b)'), 'Cost of Production:Production Supplies', 'csv-94-BMCV-WHOLESALE-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBRYANT\s+AND\s+GILBE\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bBRYANT\s+AND\s+GILBE\b)'), 'Distribution Revenue', 'csv-95-BRYANT-AND-GILBE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBUD\s+TECHNOLOGY\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bBUD\s+TECHNOLOGY\b)'), 'Cost of Inventory:Distribution Services', 'csv-96-BUD-TECHNOLOGY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCALIFORNIA\s+EXTRACTION\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCALIFORNIA\s+EXTRACTION\b)'), 'Cost of Production:Production Supplies', 'csv-98-CALIFORNIA-EXTRACTION'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCDTFA\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCDTFA\b)'), 'DeliverMD Expenses:Licenses & Permits', 'csv-99-CDTFA'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCHASE\s+CREDIT\s+CRD\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCHASE\s+CREDIT\s+CRD\b)'), 'CC 9551', 'csv-100-CHASE-CREDIT-CRD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCHEFSTORE\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCHEFSTORE\b)'), 'Cost of Production:Production Supplies', 'csv-101-CHEFSTORE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCHEVRON\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCHEVRON\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-102-CHEVRON'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCINTAS\s+CORP\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCINTAS\s+CORP\b)'), 'Cost of Production:Uniforms', 'csv-103-CINTAS-CORP'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCIRCLE\s+MUSKRAT\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCIRCLE\s+MUSKRAT\b)'), 'Distribution Revenue', 'csv-104-CIRCLE-MUSKRAT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCURRENCY\s+AND\s+COIN\s+DEPOSITED\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bCURRENCY\s+AND\s+COIN\s+DEPOSITED\b)'), 'Distribution Revenue', 'csv-105-CURRENCY-AND-COIN-DEPOSITED'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDCC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bDCC\b)'), 'Overhead Costs:Licenses & Permits', 'csv-106-DCC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDIXON\s+GAS\s+\&\s+SHOP\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bDIXON\s+GAS\s+\&\s+SHOP\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-107-DIXON-GAS-SHOP'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEASTWEST\s+BANK\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bEASTWEST\s+BANK\b)'), 'Bank Charges & Fees', 'csv-108-EASTWEST-BANK'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEBMUD\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bEBMUD\b)'), 'Overhead Costs:Utilities Expense:Water', 'csv-109-EBMUD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bENTERPRISE\s+RENT\-A\-CAR\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bENTERPRISE\s+RENT\-A\-CAR\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-110-ENTERPRISE-RENT-A-CAR'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bERACTOLL\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bERACTOLL\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-111-ERACTOLL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEUPHORIC\s+LIFE\s+INC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bEUPHORIC\s+LIFE\s+INC\b)'), 'Cost of Inventory', 'csv-112-EUPHORIC-LIFE-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEVERON\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bEVERON\s+LLC\b)'), 'Overhead Costs:Security Expense', 'csv-113-EVERON-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEXXON\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bEXXON\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-114-EXXON'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bFEDEX\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bFEDEX\b)'), 'Office/General Supplies', 'csv-115-FEDEX'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bFRANCHISE\s+TAX\s+BOARD\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bFRANCHISE\s+TAX\s+BOARD\b)'), 'Overhead Costs:Licenses & Permits', 'csv-116-FRANCHISE-TAX-BOARD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGAWFCO\s+WESTLEY\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bGAWFCO\s+WESTLEY\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-117-GAWFCO-WESTLEY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGEICO\s+\s+\*AUTO\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bGEICO\s+\s+\*AUTO\b)'), 'Smoakland Distribution Expenses:Vehicle Insurance', 'csv-118-GEICO-AUTO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGOLDEN\s+EMPIRE\s+TOWING\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bGOLDEN\s+EMPIRE\s+TOWING\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-119-GOLDEN-EMPIRE-TOWING'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGUSTO\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bGUSTO\b)'), 'Cost of Production:Direct Labor Costs', 'csv-120-GUSTO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bH\s+\&\s+L\s+PRIVATE\s+SECURITY\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bH\s+\&\s+L\s+PRIVATE\s+SECURITY\b)'), 'Overhead Costs:Security Expense', 'csv-121-H-L-PRIVATE-SECURITY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHAH\s+7\s+CA\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bHAH\s+7\s+CA\s+LLC\b)'), 'Manufacturing Revenue', 'csv-122-HAH-7-CA-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHAPPY\s+PORT\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bHAPPY\s+PORT\b)'), 'Distribution Revenue', 'csv-123-HAPPY-PORT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHARRENS\s+LAB\s+INC\.\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bHARRENS\s+LAB\s+INC\.\b)'), 'Cost of Production:Lab Testing', 'csv-124-HARRENS-LAB-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHOLLISTER\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bHOLLISTER\b)'), 'Distribution Revenue', 'csv-125-HOLLISTER'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHONGKONG\s+PORTER\s+ELECTRONICS\s+LIMITED\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bHONGKONG\s+PORTER\s+ELECTRONICS\s+LIMITED\b)'), 'Cost of Production:Packaging', 'csv-126-HONGKONG-PORTER-ELECTRONICS-LIMITED'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bINFINITY\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bINFINITY\b)'), 'Smoakland Distribution Expenses:Vehicle Insurance', 'csv-127-INFINITY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bLAZ\s+PARKING\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bLAZ\s+PARKING\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-128-LAZ-PARKING'),
    (re.compile(r"(?i)(?:\bPAYEE_VENDOR:[^|]*\bLOVE'S\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bLOVE'S\b)"), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-129-LOVE-S'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMANDELA\s+PARTNERS\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bMANDELA\s+PARTNERS\s+LLC\b)'), 'Overhead Costs:Rent Expense', 'csv-130-MANDELA-PARTNERS-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMANTECA\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bMANTECA\s+LLC\b)'), 'Distribution Revenue', 'csv-131-MANTECA-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMASH\s+GAS\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bMASH\s+GAS\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-132-MASH-GAS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMCR\s+LABS\s+\-\s+NY\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bMCR\s+LABS\s+\-\s+NY\b)'), 'Cost of Production:Lab Testing', 'csv-133-MCR-LABS-NY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMEDIWASTE\s+DISPOSAL\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bMEDIWASTE\s+DISPOSAL\s+LLC\b)'), 'Overhead Costs:Utilities Expense:Waste Disposal', 'csv-134-MEDIWASTE-DISPOSAL-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMIRAMAR\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bMIRAMAR\s+LLC\b)'), 'Distribution Revenue', 'csv-135-MIRAMAR-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMOM\s+GO\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bMOM\s+GO\s+LLC\b)'), 'Distribution Revenue', 'csv-136-MOM-GO-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bNBCU\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bNBCU\b)'), 'Bank Charges & Fees', 'csv-137-NBCU'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bODOO\s+B2B\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bODOO\s+B2B\b)'), 'Distribution Revenue', 'csv-138-ODOO-B2B'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bOIL\s+CHANGERS\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bOIL\s+CHANGERS\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-139-OIL-CHANGERS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bOLD\s+PAL\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bOLD\s+PAL\s+LLC\b)'), 'Cost of Inventory', 'csv-140-OLD-PAL-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bORINDA\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bORINDA\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-141-ORINDA'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bOTC\s+VAN\s+NUYS\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bOTC\s+VAN\s+NUYS\b)'), 'Distribution Revenue', 'csv-142-OTC-VAN-NUYS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bOTC\s+WEHO\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bOTC\s+WEHO\b)'), 'Distribution Revenue', 'csv-143-OTC-WEHO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPANOCHE\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bPANOCHE\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-144-PANOCHE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPARKINGCENTRAL\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bPARKINGCENTRAL\b)'), 'Smoakland Distribution Expenses:Distribution Vehicle Expenses', 'csv-145-PARKINGCENTRAL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPATCHWORK\s+FARMS\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bPATCHWORK\s+FARMS\b)'), 'Cost of Inventory', 'csv-146-PATCHWORK-FARMS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPAYNW\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bPAYNW\b)'), 'Cost of Production:Direct Labor Costs', 'csv-147-PAYNW'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPG\&E\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bPG\&E\b)'), 'Overhead Costs:Utilities Expense:Electricity', 'csv-148-PG-E'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPILOT\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bPILOT\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-149-PILOT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bRINSE\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bRINSE\b)'), 'Cost of Production:Uniforms', 'csv-150-RINSE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bS\&S\s+FLAVORS\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bS\&S\s+FLAVORS\b)'), 'Cost of Production:Production Supplies', 'csv-151-S-S-FLAVORS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSCZZ\s+COLLECTIVE\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bSCZZ\s+COLLECTIVE\b)'), 'Distribution Revenue', 'csv-152-SCZZ-COLLECTIVE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSHELL\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bSHELL\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-153-SHELL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSMOAKLAND\s+WEED\s+DELIVERY\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bSMOAKLAND\s+WEED\s+DELIVERY\b)'), 'Manufacturing Revenue', 'csv-154-SMOAKLAND-WEED-DELIVERY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSONOMA\s+CHO\s+LLC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bSONOMA\s+CHO\s+LLC\b)'), 'Distribution Revenue', 'csv-155-SONOMA-CHO-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSURVIVORMEDZ\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bSURVIVORMEDZ\b)'), 'Distribution Revenue', 'csv-156-SURVIVORMEDZ'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSY\s+ENTERPRISES\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bSY\s+ENTERPRISES\b)'), 'Distribution Revenue', 'csv-157-SY-ENTERPRISES'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bTABLECOVERS\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bTABLECOVERS\b)'), 'Office/General Supplies', 'csv-158-TABLECOVERS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bTHE\s+HOME\s+DEPOT\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bTHE\s+HOME\s+DEPOT\b)'), 'Cost of Production:Production Supplies', 'csv-159-THE-HOME-DEPOT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bTHE\s+NORDHOFF\s+COM\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bTHE\s+NORDHOFF\s+COM\b)'), 'Distribution Revenue', 'csv-160-THE-NORDHOFF-COM'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bU\-HAUL\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bU\-HAUL\b)'), 'Cost of Production:Moving & Storage', 'csv-161-U-HAUL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bULINE\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bULINE\b)'), 'Cost of Production:Packaging', 'csv-162-ULINE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bUNION\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bUNION\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-163-UNION'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bUSA\s+LAB\s+INC\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bUSA\s+LAB\s+INC\b)'), 'Cost of Production:Lab Testing', 'csv-164-USA-LAB-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bVALERO\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bVALERO\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-165-VALERO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bVALLEJO\s+GAS\s+\&\s+SHOP\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bVALLEJO\s+GAS\s+\&\s+SHOP\b)'), 'Smoakland Distribution Expenses:Fuel & Oil', 'csv-166-VALLEJO-GAS-SHOP'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bWASTE\s+MGMT\b.*?\bENTITY_QBO:[^|]*\bSSC\b|\bENTITY_QBO:[^|]*\bSSC\b.*?\bPAYEE_VENDOR:[^|]*\bWASTE\s+MGMT\b)'), 'Overhead Costs:Utilities Expense:Waste Disposal', 'csv-167-WASTE-MGMT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAEROPAY\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bAEROPAY\b)'), 'Bank Charges & Fees', 'csv-168-AEROPAY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAPPFOLIO,\s+INC\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bAPPFOLIO,\s+INC\b)'), 'DeliverMD Expenses:Office/General Supplies', 'csv-170-APPFOLIO-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bARMORED\s+CAR\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bARMORED\s+CAR\b)'), 'Bank Charges & Fees', 'csv-171-ARMORED-CAR'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBLAZE\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bBLAZE\b)'), 'Delivery COGS:Apps & Software', 'csv-173-BLAZE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCDTFA\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bCDTFA\b)'), 'CDTFA Taxes', 'csv-174-CDTFA'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+OAKLAND\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+OAKLAND\b)'), 'Facility Costs:Licenses & Permits', 'csv-175-CITY-OF-OAKLAND'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEBMUD\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bEBMUD\b)'), 'Facility Costs:Utilities', 'csv-176-EBMUD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bFASTRAK\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bFASTRAK\b)'), 'DeliverMD Expenses:Delivery Vehicle Expenses:Other Expenses', 'csv-177-FASTRAK'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bH\s+\&\s+L\s+PRIVATE\s+SECURITY\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bH\s+\&\s+L\s+PRIVATE\s+SECURITY\b)'), 'Facility Costs:Security Expense', 'csv-178-H-L-PRIVATE-SECURITY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHONEY\s+BUCKET\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bHONEY\s+BUCKET\b)'), 'Facility Costs:Security Expense', 'csv-179-HONEY-BUCKET'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bINTELLIWORX\s+PH\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bINTELLIWORX\s+PH\b)'), 'DeliverMD Expenses:Consultant and Professional Fees', 'csv-180-INTELLIWORX-PH'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bNBCU\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bNBCU\b)'), 'Bank Charges & Fees', 'csv-181-NBCU'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bONFLEET\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bONFLEET\b)'), 'Delivery COGS:Vehicle Compliance', 'csv-182-ONFLEET'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bOPC\s+CROS\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bOPC\s+CROS\b)'), 'Bank Charges & Fees', 'csv-183-OPC-CROS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPAYNW\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bPAYNW\b)'), 'Delivery COGS:Labor Cost', 'csv-184-PAYNW'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPG\&E\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bPG\&E\b)'), 'Facility Costs:Utilities', 'csv-185-PG-E'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSMOAKLAND\s+SUPPLY\s+CORP\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bSMOAKLAND\s+SUPPLY\s+CORP\b)'), 'Cost of Inventory', 'csv-186-SMOAKLAND-SUPPLY-CORP'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSTRONGHOLD\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bSTRONGHOLD\b)'), 'Delivery Revenue', 'csv-187-STRONGHOLD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bWEINSTEIN\s+LOCAL\b.*?\bENTITY_QBO:[^|]*\bSWD\b|\bENTITY_QBO:[^|]*\bSWD\b.*?\bPAYEE_VENDOR:[^|]*\bWEINSTEIN\s+LOCAL\b)'), 'Facility Costs:Rent Expense', 'csv-188-WEINSTEIN-LOCAL'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bAEROPAY\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bAEROPAY\b)'), 'Smoakland SoCal Expense:Bank Charges & Fees', 'csv-189-AEROPAY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBLAZE\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bBLAZE\b)'), 'Smoakland SoCal Expense:Software & Apps', 'csv-191-BLAZE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+PORT\s+HUENEME\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bCITY\s+OF\s+PORT\s+HUENEME\b)'), 'Smoakland SoCal Expense:City Tax', 'csv-192-CITY-OF-PORT-HUENEME'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEDISON\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bEDISON\b)'), 'Smoakland SoCal Expense:Utilities', 'csv-193-EDISON'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGUSTO\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bGUSTO\b)'), 'Smoakland SoCal Expense:Labor Cost', 'csv-194-GUSTO'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bISLAMIC\s+CENTER\s+OF\s+VENTURA\s+COUNTY\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bISLAMIC\s+CENTER\s+OF\s+VENTURA\s+COUNTY\b)'), 'Smoakland SoCal Expense:Bank Charges & Fees', 'csv-195-ISLAMIC-CENTER-OF-VENTURA-COUNTY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMONTHLY\s+MAINTENANCE\s+FEE\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bMONTHLY\s+MAINTENANCE\s+FEE\b)'), 'Smoakland SoCal Expense:Bank Charges & Fees', 'csv-196-MONTHLY-MAINTENANCE-FEE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bRA\s+\&\s+BL\s+BEGGS\s+TRUST\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bRA\s+\&\s+BL\s+BEGGS\s+TRUST\b)'), 'Smoakland SoCal Expense:Rent Expense', 'csv-197-RA-BL-BEGGS-TRUST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSECURITY\s+MARKETING\s+KING\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bSECURITY\s+MARKETING\s+KING\b)'), 'Smoakland SoCal Expense:Security Expenses', 'csv-198-SECURITY-MARKETING-KING'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSENTRY\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bSENTRY\b)'), 'Smoakland SoCal Expense:Security Expenses', 'csv-199-SENTRY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bSTRONGHOLD\b.*?\bENTITY_QBO:[^|]*\bSOCAL\b|\bENTITY_QBO:[^|]*\bSOCAL\b.*?\bPAYEE_VENDOR:[^|]*\bSTRONGHOLD\b)'), 'Smoakland SoCal Expense:Bank Charges & Fees', 'csv-200-STRONGHOLD'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\b2\s+FOREST\s+PARK\s+LANE\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\b2\s+FOREST\s+PARK\s+LANE\b)'), 'NY Distribution Revenue', 'csv-202-2-FOREST-PARK-LANE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bBLAZE\s+420\s+LLC\s+27\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bBLAZE\s+420\s+LLC\s+27\b)'), 'NY Distribution Revenue', 'csv-203-BLAZE-420-LLC-27'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCAFFIEND\s+THE\s+BUSHWICK\s+DISPENSARY\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bCAFFIEND\s+THE\s+BUSHWICK\s+DISPENSARY\b)'), 'NY Distribution Revenue', 'csv-204-CAFFIEND-THE-BUSHWICK-DISPENSARY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCAPITAL\s+DISTRICT\s+CANNABIS\s+AND\s+WELLNESS\s+INC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bCAPITAL\s+DISTRICT\s+CANNABIS\s+AND\s+WELLNESS\s+INC\b)'), 'NY Distribution Revenue', 'csv-205-CAPITAL-DISTRICT-CANNABIS-AND-WELLNESS-I'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCASE\s+MANAGEMENT\s+INC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bCASE\s+MANAGEMENT\s+INC\b)'), 'NY Distribution Revenue', 'csv-206-CASE-MANAGEMENT-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bCHANG\s+YI\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bCHANG\s+YI\b)'), 'Smoakland New York Expense:Commissions', 'csv-207-CHANG-YI'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bDUTCHESS\s+ROOTS\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bDUTCHESS\s+ROOTS\b)'), 'NY Distribution Revenue', 'csv-208-DUTCHESS-ROOTS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bE\-Z\*PASS\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bE\-Z\*PASS\b)'), 'Smoakland New York Expense:Vehicle Expense', 'csv-209-E-Z-PASS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bEXIT\s+31\s+EXOTIC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bEXIT\s+31\s+EXOTIC\b)'), 'NY Distribution Revenue', 'csv-210-EXIT-31-EXOTIC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bFRESHLY\s+BAKED\s+NYC\s+2\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bFRESHLY\s+BAKED\s+NYC\s+2\b)'), 'NY Distribution Revenue', 'csv-211-FRESHLY-BAKED-NYC-2'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGREENERY\s+SPOT\s+LLC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bGREENERY\s+SPOT\s+LLC\b)'), 'NY Distribution Revenue', 'csv-212-GREENERY-SPOT-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bGUARDIAN\s+WELLNESS\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bGUARDIAN\s+WELLNESS\b)'), 'NY Distribution Revenue', 'csv-213-GUARDIAN-WELLNESS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHAPPY\s+DAYS\s+DISPENSARY\s+INC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bHAPPY\s+DAYS\s+DISPENSARY\s+INC\b)'), 'NY Distribution Revenue', 'csv-214-HAPPY-DAYS-DISPENSARY-INC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bHERB\-Z\s+LLC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bHERB\-Z\s+LLC\b)'), 'NY Distribution Revenue', 'csv-215-HERB-Z-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMARCO\s+FEI\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bMARCO\s+FEI\b)'), 'Smoakland New York Expense:Rent Expense', 'csv-216-MARCO-FEI'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bMONTHLY\s+MAINTENANCE\s+FEE\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bMONTHLY\s+MAINTENANCE\s+FEE\b)'), 'Smoakland New York Expense:Bank Charges & Fees', 'csv-217-MONTHLY-MAINTENANCE-FEE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bNISSAN\s+FINANCE\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bNISSAN\s+FINANCE\b)'), 'Smoakland New York Expense:Vehicle Expense', 'csv-218-NISSAN-FINANCE'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bNSEW\s+TRADING\s+COMPANY\s+LLC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bNSEW\s+TRADING\s+COMPANY\s+LLC\b)'), 'NY Distribution Revenue', 'csv-219-NSEW-TRADING-COMPANY-LLC'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bNUCLEUS\s+DISPENSARY\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bNUCLEUS\s+DISPENSARY\b)'), 'NY Distribution Revenue', 'csv-220-NUCLEUS-DISPENSARY'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bNUNA\s+HARVEST\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bNUNA\s+HARVEST\b)'), 'NY Distribution Revenue', 'csv-221-NUNA-HARVEST'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bPROGRESSIVE\s+INS\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bPROGRESSIVE\s+INS\b)'), 'Smoakland New York Expense:Vehicle Insurance', 'csv-222-PROGRESSIVE-INS'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bTHE\s+PEOPLES\s+JOINT\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bTHE\s+PEOPLES\s+JOINT\b)'), 'NY Distribution Revenue', 'csv-223-THE-PEOPLES-JOINT'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bTWENTY8GRAMZ\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bTWENTY8GRAMZ\b)'), 'NY Distribution Revenue', 'csv-224-TWENTY8GRAMZ'),
    (re.compile(r'(?i)(?:\bPAYEE_VENDOR:[^|]*\bWHITEBOXTHC\s+LLC\b.*?\bENTITY_QBO:[^|]*\bNY\b|\bENTITY_QBO:[^|]*\bNY\b.*?\bPAYEE_VENDOR:[^|]*\bWHITEBOXTHC\s+LLC\b)'), 'NY Distribution Revenue', 'csv-225-WHITEBOXTHC-LLC'),
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
    (re.compile(r'(?i)(?<![A-Z0-9])(?:BUTTONWILLOW|UNION|FUEL\ \&\ OIL|SHELL|CHEVRON|76\b|AMOCO|VALERO)(?![A-Z0-9])'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL'),
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
    (re.compile(r'(?i)\bPAYEE_VENDOR:[^|]*\bAPPFOLIO(?:,?\s+INC\.?)?\b'), 'DELIVERMD EXPENSES:OFFICE/GENERAL SUPPLIES'),
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

# We concatenate the same canonical fields we've been using across rulebooks.
_SOURCE_COLS: List[str] = [
    "bank_account", "bank_cc_num", "payee_vendor", "entity_qbo",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.upper().strip()
    return " ".join(s.split())

_SOURCE_COLS: List[str] = [
    "payee_vendor",
    "bank_account_cc",
    "amount",
    "cf_account",
    "dashboard_1",
    "entity_qbo",
    "bank_account",
    "bank_cc_num",
]

def _concat_row(row: Dict) -> str:
    parts: List[str] = []

    for c in _SOURCE_COLS:
        v = row.get(c, "") or ""
        v = str(v)
        if v.strip():
            parts.append(f"{c.upper()}: {v}")

    bank_account = str(row.get("bank_account", "") or "").strip()
    bank_cc = str(row.get("bank_cc_num", "") or "").strip()
    if bank_account and bank_cc:
        parts.append(f"BANK_ACCOUNT: {bank_account} {bank_cc}")
        parts.append(f"BANK_CC_NUM: {bank_account} {bank_cc}")

    return _normalize_text(" | ".join(parts))

_POSTPROCESS_SOURCE = "postprocess"

def postprocess(df: "pd.DataFrame") -> "pd.DataFrame":  # type: ignore
    if df is None or df.empty:
        return df

    need = {"payee_vendor", "amount", "entity_qbo", "qbo_account"}
    if not need.issubset(df.columns):
        return df

    out = df.copy()
    amt = pd.to_numeric(out["amount"], errors="coerce")
    payee = out["payee_vendor"].fillna("").astype(str).str.upper()
    ent = out["entity_qbo"].fillna("").astype(str).str.upper()

    # usa bank_account_cc si lo tienes; si no, ajusta a BANK_ACCOUNT/BANK_CC_NUM pre-mapeado en tu pipeline
    acct = out["bank_account_cc"].fillna("").astype(str).str.upper() if "bank_account_cc" in out.columns else pd.Series([""]*len(out))

    def stamp(mask, value, rule_id: str):
        if not mask.any():
            return
        out.loc[mask, "qbo_account"] = (value or "").upper()
        if "qbo_account_rule_tag" in out.columns:
            out.loc[mask, "qbo_account_rule_tag"] = f"{RULEBOOK_NAME}@{RULEBOOK_VERSION}#{rule_id}"
        if "qbo_account_source" in out.columns:
            out.loc[mask, "qbo_account_source"] = _POSTPROCESS_SOURCE
        if "qbo_account_confidence" in out.columns:
            out.loc[mask, "qbo_account_confidence"] = 1.0

    # Aeropay (por entidad + bank + signo)
    aer = payee.eq("AEROPAY")
    stamp(aer & ent.eq("SOCAL") & acct.eq("DAMA 5597") & (amt > 0), "SoCal Delivery Revenue", "pp-aeropay-socal-5597-pos")
    stamp(aer & ent.eq("SOCAL") & acct.eq("DAMA 5597") & (amt < 0), "Smoakland SoCal Expense:Bank Charges & Fees", "pp-aeropay-socal-5597-neg")

    stamp(aer & ent.eq("HAH 7 CA") & acct.eq("KP 6852") & (amt > 0), "Delivery Revenue:Cannabis", "pp-aeropay-hah-6852-pos")
    stamp(aer & ent.eq("HAH 7 CA") & acct.eq("KP 6852") & (amt < 0), "Bank/Armor Fees", "pp-aeropay-hah-6852-neg")

    stamp(aer & ent.eq("SSC") & acct.eq("NBCU 2035") & (amt > 0), "Delivery Revenue", "pp-aeropay-ssc-2035-pos")
    stamp(aer & ent.eq("SSC") & acct.eq("NBCU 2035") & (amt < 0), "Bank Charges & Fees", "pp-aeropay-ssc-2035-neg")

    stamp(aer & ent.eq("SWD") & acct.eq("NBCU SWD") & (amt > 0), "Delivery Revenue", "pp-aeropay-swd-swd-pos")
    stamp(aer & ent.eq("SWD") & acct.eq("NBCU SWD") & (amt < 0), "Bank Charges & Fees", "pp-aeropay-swd-swd-neg")

    # Armored Car
    arm = payee.eq("ARMORED CAR")
    stamp(arm & (amt > 0), "Delivery Revenue", "pp-armored-car-pos")
    stamp(arm & (amt < 0), "Bank Charges & Fees", "pp-armored-car-neg")

    # Bud Technology
    bud = payee.eq("BUD TECHNOLOGY")
    stamp(bud & (amt > 0), "Distribution Revenue", "pp-bud-pos")
    stamp(bud & (amt < 0), "Cost of Inventory:Distribution Services", "pp-bud-neg")

    # Odoo B2B + positive
    odoo = payee.eq("ODOO B2B")
    stamp(odoo & (amt > 0), "Distribution Revenue", "pp-odoo-pos")

    # OSS
    oss = payee.eq("OSS")
    stamp(oss & (amt > 0), "Delivery Revenue:Cannabis", "pp-oss-pos")
    stamp(oss & (amt < 0), "Bank/Armor Fees", "pp-oss-neg")

    # Blaze exact ±1050
    blaze = payee.eq("BLAZE") & (amt.abs() == 1050.00)
    stamp(blaze, "Software & Apps", "pp-blaze-1050")

    # Xeven exact ±3200
    xev = payee.eq("XEVEN SOLUTIONS") & (amt.abs() == 3200.00)
    stamp(xev, "Smoakland Media Expense:Advertising and Promotion", "pp-xeven-3200")

    return out

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
            return (value.upper(), _rule_tag(i, custom_id))

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
    return postprocess(out)
