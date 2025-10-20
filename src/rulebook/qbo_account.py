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
    (re.compile(r'(?i)\bX0435\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bX0586\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bADVERTISING\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bPROMOTION\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bSMOAKLANDSAC\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bCOIN\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bCURRENCY\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bPLIVO\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bPLIVO\.COM\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bX0000\b'), 'DIRECT LABOR COST', None),
    (re.compile(r'(?i)\bOTC\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bEMPYREAL\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bEMPYREALENTERPRI\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bBILLB\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bA35810\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bDNS\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bLHI\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bWEINSTEIN\b'), 'FACILITY COSTS:RENT EXPENSE', None),
    (re.compile(r'(?i)\bBUCKET\b'), 'FACILITY COSTS:SECURITY EXPENSE', None),
    (re.compile(r'(?i)\bHONEY\b'), 'FACILITY COSTS:SECURITY EXPENSE', None),
    (re.compile(r'(?i)\bCABBAGE\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bAMZN\.COM\b'), 'TPH786 EXPENSES:OFFICE EXPENSES', None),
    (re.compile(r'(?i)\bECHTRAI\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bAIRCALL\.IO\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bMSFT\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bCITI\b'), 'DUE TO TPH786', None),
    (re.compile(r'(?i)\bDISPOSAL\b'), 'OVERHEAD COSTS:UTILITIES EXPENSE:WASTE DISPOSAL', None),
    (re.compile(r'(?i)\bMEDIWASTE\b'), 'OVERHEAD COSTS:UTILITIES EXPENSE:WASTE DISPOSAL', None),
    (re.compile(r'(?i)\bPGANDE\b'), 'FACILITY COSTS:UTILITIES', None),
    (re.compile(r'(?i)\bCANVA\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bANALYTIC\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bKONCAUSEWAY\b'), 'COST OF PRODUCTION:PACKAGING', None),
    (re.compile(r'(?i)\bMOM\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bAPPFOLIO\b'), 'DELIVERMD EXPENSES:OFFICE/GENERAL SUPPLIES', None),
    (re.compile(r'(?i)\bORIN\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bCONFECTIONS\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bNORTHWEST\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bDNH\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bADT\b'), 'OVERHEAD COSTS:SECURITY EXPENSE', None),
    (re.compile(r'(?i)\bCREATIVE\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bSPECTRUM\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bFEI\b'), 'SMOAKLAND NEW YORK EXPENSE:RENT EXPENSE', None),
    (re.compile(r'(?i)\bMARCO\b'), 'SMOAKLAND NEW YORK EXPENSE:RENT EXPENSE', None),
    (re.compile(r'(?i)\bWORLDWIDE\b'), 'BANK/ARMOR FEES', None),
    (re.compile(r'(?i)\bADS7475396058\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bACCOUNTFEE\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bPASS\b'), 'SMOAKLAND NEW YORK EXPENSE:VEHICLE EXPENSE', None),
    (re.compile(r'(?i)\bPASSNY\b'), 'SMOAKLAND NEW YORK EXPENSE:VEHICLE EXPENSE', None),
    (re.compile(r'(?i)\bCHEVLEBEC\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bCLOVER\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bACROPRO\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bCOMETCHAT\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bSUBS\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bPMNT\b'), 'FACILITY COSTS:PROPERTY INSURANCE', None),
    (re.compile(r'(?i)\bSTONEMARK\b'), 'FACILITY COSTS:PROPERTY INSURANCE', None),
    (re.compile(r'(?i)\bSMOAKLANDPORTH\b'), 'SOCAL DELIVERY REVENUE', None),
    (re.compile(r'(?i)\bLC2400\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bALKHEMIST\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bIOT\b'), 'DELIVERY COGS:VEHICLE COMPLIANCE', None),
    (re.compile(r'(?i)\bWHSE\b'), 'DELIVERMD EXPENSES:OFFICE/GENERAL SUPPLIES', None),
    (re.compile(r'(?i)\bKUSHMEN\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bACHTRANS\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bXPRESSCPTL\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bPRIMO\b'), 'OVERHEAD COSTS:UTILITIES EXPENSE:WATER', None),
    (re.compile(r'(?i)\bWTH\b'), 'BANK/ARMOR FEES', None),
    (re.compile(r'(?i)\bSQJERSEY\b'), 'TPH786 EXPENSES:TRAVEL EXPENSE', None),
    (re.compile(r'(?i)\bGODADDY\.COM\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bDISPENSARY\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bSTATIOOAKLAND\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bSAC\.HDLGOV\b'), 'CITY OF SACRAMENTO TAXES', None),
    (re.compile(r'(?i)\bSACRAMEN\b'), 'CITY OF SACRAMENTO TAXES', None),
    (re.compile(r'(?i)\bHONEYBUCKET\b'), 'FACILITY COSTS:SECURITY EXPENSE', None),
    (re.compile(r'(?i)\bPEWESTLEY\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bECOGREENINDUSTRIES\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bECOGREENINDUSTRIES\.COM\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bBRYANT\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bGILBE\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bBEGGS\b'), 'SMOAKLAND SOCAL EXPENSE:RENT EXPENSE', None),
    (re.compile(r'(?i)\bOVERHEAD\b'), 'DUE FROM SUBLIME', None),
    (re.compile(r'(?i)\b079259SAN\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:DISTRIBUTION VEHICLE EXPENSES', None),
    (re.compile(r'(?i)\bZOOM\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bZOOM\.US\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bFARMS\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bPATCHWORK\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bFRONT\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bGODADDY\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\b4ZN2A7PJMYQD\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bINVOICES\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bLOCATIONS\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bSCZZ\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bBAKESFIELD\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bHIGHLAND\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bPURCHASE09100001\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bMUSKRAT\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bPACAFI\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bPCF\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bDIRECT\b'), 'DUE FROM SUBLIME', None),
    (re.compile(r'(?i)\bLABOR\b'), 'DUE FROM SUBLIME', None),
    (re.compile(r'(?i)\bSTATIOORINDA\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bPATIENT\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bGREENBAXMRB\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bRIVERSIDE\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bRETAIL_PAY\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:REPAIRS AND MAINTENANCE', None),
    (re.compile(r'(?i)\bCELL\b'), 'BANK/ARMOR FEES', None),
    (re.compile(r'(?i)\bWIDE\b'), 'BANK/ARMOR FEES', None),
    (re.compile(r'(?i)\bASHS\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bINDUS\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bSLO\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bINS\.PMTS\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:AUTO INSURANCE', None),
    (re.compile(r'(?i)\bNEXT\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:AUTO INSURANCE', None),
    (re.compile(r'(?i)\bPREMIU\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:AUTO INSURANCE', None),
    (re.compile(r'(?i)\bWAVE\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:AUTO INSURANCE', None),
    (re.compile(r'(?i)\bNET\b'), 'SMOAKLAND SOCAL EXPENSE:LABOR COST', None),
    (re.compile(r'(?i)\bREM\b'), 'SMOAKLAND SOCAL EXPENSE:LABOR COST', None),
    (re.compile(r'(?i)\bNETWOR\b'), 'OFFICE/GENERAL SUPPLIES', None),
    (re.compile(r'(?i)\bC043488NEWARK\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:DISTRIBUTION VEHICLE EXPENSES', None),
    (re.compile(r'(?i)\bCHEVWESTLEY\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bXXXXXXXXXXXX8630\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bDOPE\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bSPRINGBIG\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bGREENBAXDEBITCAR\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bANTIOCH\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bCENT\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bHEALING\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bKIM\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bTIGRANLEVONANDLA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bCONSULTING\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bFHL\b'), 'DELIVERY REVENUE', None),
    (re.compile(r'(?i)\b08\.15\.2023\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bHOLDING\b'), 'MANAGEMENT FEE EXPENSE', None),
    (re.compile(r'(?i)\bOPSMO\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bDROPBOX\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bSEMRUSH\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bNORDHOFF\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bMEDIAJEL\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bPTB\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bSYS\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bAVISNYCFEE\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:DISTRIBUTION VEHICLE EXPENSES', None),
    (re.compile(r'(?i)\bONBOARD\b'), 'TPH786 EXPENSES:TRAVEL EXPENSE', None),
    (re.compile(r'(?i)\bARIEL\b'), 'DIRECT LABOR COSTS', None),
    (re.compile(r'(?i)\bADCHEM\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bCKO\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bXXXXXXXXXXXX7706\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bDEMAND\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bSTEADY\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bMANTECA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bMONTEREY\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bYOUNG\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bXFINITY\b'), 'SMOAKLAND MEDIA EXPENSE:INTERNET & TELEPHONE EXPENSE', None),
    (re.compile(r'(?i)\bBAKED\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bDISPE\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bFRESHLY\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bNSEW\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bNYC\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bTRADING\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bPASAN\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:DISTRIBUTION VEHICLE EXPENSES', None),
    (re.compile(r'(?i)\bHIS2000442\b'), 'TPH786 EXPENSES:INSURANCE', None),
    (re.compile(r'(?i)\bOYAMASUSH\b'), 'TPH786 EXPENSES:MEALS / LUNCHES', None),
    (re.compile(r'(?i)\bARORA\b'), 'CANNABIS SALES', None),
    (re.compile(r'(?i)\bPRIMPATCHARA\b'), 'CANNABIS SALES', None),
    (re.compile(r'(?i)\bF54C43DBBDB1\b'), 'DELIVERY COGS:DIRECT LABOR COST', None),
    (re.compile(r'(?i)\bMELISSA\b'), 'DIRECT LABOR COSTS', None),
    (re.compile(r'(?i)\bEARTHWISE\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bSHOPIFY\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bCORONA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bINDIO\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bSENDER\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bJPS\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bSENDGRID\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bPAYROLLSYS\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bFINANCING\b'), 'FACILITY COSTS:PROPERTY INSURANCE', None),
    (re.compile(r'(?i)\bPFS\b'), 'FACILITY COSTS:PROPERTY INSURANCE', None),
    (re.compile(r'(?i)\bJATO\b'), 'TPH786 EXPENSES:LEGAL FEES', None),
    (re.compile(r'(?i)\bISLAMIC\b'), 'SMOAKLAND SOCAL EXPENSE:BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bGUARDIAN\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bEVERON\b'), 'OVERHEAD COSTS:SECURITY EXPENSE', None),
    (re.compile(r'(?i)\bSAIGONHOU\b'), 'TPH786 EXPENSES:MEALS / LUNCHES', None),
    (re.compile(r'(?i)\bTST\b'), 'TPH786 EXPENSES:MEALS / LUNCHES', None),
    (re.compile(r'(?i)\bEXTRACTION\b'), 'COST OF PRODUCTION:PRODUCTION SUPPLIES', None),
    (re.compile(r'(?i)\bTARGET\.COM\b'), 'TPH786 EXPENSES:OFFICE EXPENSES', None),
    (re.compile(r'(?i)\bSTATIOCASTAIC\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bBLV\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bOBATA\b'), 'DIRECT LABOR COSTS', None),
    (re.compile(r'(?i)\bBAKEFIELDS\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bPURPLE\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bFAX\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bTIME\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bX0001\b'), 'DIRECT LABOR COST', None),
    (re.compile(r'(?i)\bX1182\b'), 'DIRECT LABOR COST', None),
    (re.compile(r'(?i)\bANA\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\b300\.00\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bANT\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bBANYAN\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bCHO\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bFLORA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bFLORATERRA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bSONOMA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bWEHO\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bBACKGROUND\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bBARR\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bGOMES\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bIRMA\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bLEONIDAS\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bMICHAEL\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bHONOR\b'), 'TPH786 EXPENSES:CONSULTANT AND PROFESSIONAL FEES', None),
    (re.compile(r'(?i)\bBILL_PAY\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bEXIT\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bEXOTIC\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bNUCLEUS\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bTWENTY8GRAMZ\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bWINGSTOP\b'), 'TPH786 EXPENSES:MEALS / LUNCHES', None),
    (re.compile(r'(?i)\bPIN\b'), 'DELIVERMD EXPENSES:OFFICE/GENERAL SUPPLIES', None),
    (re.compile(r'(?i)\bRETA\b'), 'DELIVERMD EXPENSES:OFFICE/GENERAL SUPPLIES', None),
    (re.compile(r'(?i)\b00000WESTLEY\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bARCBUTTONWILLOW\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bCHEVBERKELEY\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bCHEVORINDA\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bFOUR\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bZIPPY\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bBRAND\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bFROOT\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bLUCKY\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bSHENZHEN\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bWEBSTAURANT\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bGROWTH\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bMICROSOFT\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\b3QHK\b'), 'DIRECT LABOR COST', None),
    (re.compile(r'(?i)\bACDFEE\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bARMOREDCAR\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bDATED\b'), 'BANK CHARGES & FEES', None),
    (re.compile(r'(?i)\bARTESIA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bBLOOM\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bCALITA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bEXTRACT\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bHILIFE\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bMEGANS\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bNICE\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bTHIRD\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bWELLNES\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bEUNOIA\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bTRINITY\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bDEPOSITS\b'), 'DELIVERY REVENUE', None),
    (re.compile(r'(?i)\bSAEFONG\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bBRIANA\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bJAVEN\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bJOHNSON\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bKANDY\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bMARIA\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bMORALES\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bYENI\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bDEPOSITBRIDGEPLUS\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bSCHG\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bSERIAL\b'), 'DIRECT LABOR COST', None),
    (re.compile(r'(?i)\bCASHLESS\b'), 'DELIVERY REVENUE', None),
    (re.compile(r'(?i)\bTELEPHONE\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bOAK\b'), 'DELIVERY REVENUE', None),
    (re.compile(r'(?i)\bDEPOSITED\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bPAYNW\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bADS\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bXXXXXXXXXXXX8205\b'), 'TPH786 EXPENSES:OFFICE EXPENSES', None),
    (re.compile(r'(?i)\bDEPOS\b'), 'DELIVERY REVENUE:CANNABIS', None),
    (re.compile(r'(?i)\bAIRCALL\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bONFLEET\b'), 'DELIVERY COGS:VEHICLE COMPLIANCE', None),
    (re.compile(r'(?i)\bGPS\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bGSUITE\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bHONG\b'), 'COST OF PRODUCTION:PACKAGING', None),
    (re.compile(r'(?i)\bODOO\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bCLOUD\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bMOMENTUM\b'), 'DELIVERY COGS:VEHICLE COMPLIANCE', None),
    (re.compile(r'(?i)\bDOORDASH\b'), 'TPH786 EXPENSES:MEALS / LUNCHES', None),
    (re.compile(r'(?i)\bREBILL\b'), 'SMOAKLAND NEW YORK EXPENSE:VEHICLE EXPENSE', None),
    (re.compile(r'(?i)\bATLASSIAN\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bTRELLO\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bTRELLO\.COM\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bFLORALS\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bLOCAL\b'), 'FACILITY COSTS:RENT EXPENSE', None),
    (re.compile(r'(?i)\bDINING\b'), 'TPH786 EXPENSES:MEALS / LUNCHES', None),
    (re.compile(r'(?i)\bSPIRIT\b'), 'TPH786 EXPENSES:TRAVEL EXPENSE', None),
    (re.compile(r'(?i)\b08\.01\.2023\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bLONG\b'), 'DISTRIBUTION COGS:LAB TESTING', None),
    (re.compile(r'(?i)\bROOTS\b'), 'DUE FROM TPH786', None),
    (re.compile(r'(?i)\bAIRL\b'), 'TPH786 EXPENSES:TRAVEL EXPENSE', None),
    (re.compile(r'(?i)\bINBOUND\b'), 'NY DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bBROWSERSTACK\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bBROWSERSTACK\.COM\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bBADUA\b'), 'CANNABIS SALES', None),
    (re.compile(r'(?i)\bMARISA\b'), 'CANNABIS SALES', None),
    (re.compile(r'(?i)\bRECEIVED\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bCONFIRMED\b'), 'NORTH BAY CREDIT UNION CHECKING 2035', None),
    (re.compile(r'(?i)\bPICKUP\b'), 'NORTH BAY CREDIT UNION CHECKING 2035', None),
    (re.compile(r'(?i)\bDELIGHTED\b'), 'SMOAKLAND MEDIA EXPENSE:SOFTWARE & APPS', None),
    (re.compile(r'(?i)\bAIRLINES\b'), 'TPH786 EXPENSES:TRAVEL EXPENSE', None),
    (re.compile(r'(?i)\bFACILITY\b'), 'DUE FROM HAH 7 LLC', None),
    (re.compile(r'(?i)\bFED\b'), 'CANNABIS SALES', None),
    (re.compile(r'(?i)\bFAMILY\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bSMO\b'), 'COST OF PRODUCTION:DIRECT LABOR COSTS', None),
    (re.compile(r'(?i)\bOLD\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bPAL\b'), 'COST OF INVENTORY', None),
    (re.compile(r'(?i)\bMAITLANDM\b'), 'TPH786 EXPENSES:MEALS / LUNCHES', None),
    (re.compile(r'(?i)\bWORLD\b'), 'BANK/ARMOR FEES', None),
    (re.compile(r'(?i)\bADS3407900941\b'), 'SMOAKLAND MEDIA EXPENSE:ADVERTISING AND PROMOTION', None),
    (re.compile(r'(?i)\bSPENCER\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bB2B\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bLABELS\b'), 'DISTRIBUTION COGS:PACKAGING', None),
    (re.compile(r'(?i)\bSAMSARA\b'), 'DUE FROM HAH 7 LLC', None),
    (re.compile(r'(?i)\bMCGRIFF\b'), 'COST OF PRODUCTION:DIRECT LABOR COSTS', None),
    (re.compile(r'(?i)\bTWITCHTVDON\b'), 'TPH786 EXPENSES:CONSULTANT FEES', None),
    (re.compile(r'(?i)\bEXXONMOBIL\b'), 'SMOAKLAND DISTRIBUTION EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\bAREA\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\bFASTRAK\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:OTHER EXPENSES', None),
    (re.compile(r'(?i)\bHISIG\b'), 'TPH786 EXPENSES:INSURANCE', None),
    (re.compile(r'(?i)\bSTATION\b'), 'DELIVERMD EXPENSES:DELIVERY VEHICLE EXPENSES:FUEL & OIL', None),
    (re.compile(r'(?i)\b08\.14\.2023\b'), 'DELIVERY COGS:LABOR COST', None),
    (re.compile(r'(?i)\bHIS2000444\b'), 'TPH786 EXPENSES:INSURANCE', None),
    (re.compile(r'(?i)\bHIS2000461\b'), 'TPH786 EXPENSES:INSURANCE', None),
    (re.compile(r'(?i)\bTIRE\b'), 'DUE FROM DMD', None),
    (re.compile(r'(?i)\b100\.00\b'), 'DISTRIBUTION REVENUE', None),
    (re.compile(r'(?i)\b800\.00\b'), 'DISTRIBUTION REVENUE', None),
]

# We concatenate the same canonical fields we’ve been using across rulebooks.
_SOURCE_COLS: List[str] = [
    "bank_account", "subentity", "bank_cc_num",
    "payee_vendor", "cf_account", "dashboard_1", "budget_owner",
    "entity_qbo", "qbo_sub_account",
    "description", "extended_description",
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
