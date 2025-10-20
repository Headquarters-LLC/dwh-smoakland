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
RULEBOOK_VERSION = "2025.10.03"   # <-- bump when rules change
UNKNOWN = "UNKNOWN"

_RULES: List[Tuple[re.Pattern, str, str | None]] = [
    (re.compile(r'(?i)\bDENNIS\b'), 'TPH'),
    (re.compile(r'(?i)\bADVERTISING\b'), 'TPH'),
    (re.compile(r'(?i)\bPROMOTION\b'), 'TPH'),
    (re.compile(r'(?i)\bX0435\b'), 'HAH'),
    (re.compile(r'(?i)\bX0586\b'), 'HAH'),
    (re.compile(r'(?i)\bEXECUTIVE\b'), 'TPH'),
    (re.compile(r'(?i)\bJAY\b'), 'TPH'),
    (re.compile(r'(?i)\bSMOAKLANDSAC\b'), 'HAH 7 CA'),
    (re.compile(r'(?i)\bADOBE\b'), 'TPH'),
    (re.compile(r'(?i)\bINTUIT\b'), 'TPH'),
    (re.compile(r'(?i)\bQUICKBOOKS\b'), 'TPH'),
    (re.compile(r'(?i)\bQBOOKS\b'), 'TPH'),
    (re.compile(r'(?i)\bLUNCHES\b'), 'TPH'),
    (re.compile(r'(?i)\bGOOGLE\b'), 'TPH'),
    (re.compile(r'(?i)\bPHONE\b'), 'TPH'),
    (re.compile(r'(?i)\bKLAVIYO\b'), 'TPH'),
    (re.compile(r'(?i)\bCABLE\b'), 'TPH'),
    (re.compile(r'(?i)\bCOMCAST\b'), 'TPH'),
    (re.compile(r'(?i)\bGHOST\b'), 'TPH'),
    (re.compile(r'(?i)\bPLIVO\b'), 'TPH'),
    (re.compile(r'(?i)\bPLIVO\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bONL\b'), 'TPH'),
    (re.compile(r'(?i)\bX0000\b'), 'HAH'),
    (re.compile(r'(?i)\bNULL\b'), 'TPH'),
    (re.compile(r'(?i)\bBILLB\b'), 'TPH'),
    (re.compile(r'(?i)\bA35810\b'), 'HAH'),
    (re.compile(r'(?i)\bDNS\b'), 'HAH'),
    (re.compile(r'(?i)\bDOMESTIC\b'), 'HAH'),
    (re.compile(r'(?i)\bJOINTCOMMERCE\b'), 'TPH'),
    (re.compile(r'(?i)\bTMOBILE\b'), 'TPH'),
    (re.compile(r'(?i)\bLHI\b'), 'TPH'),
    (re.compile(r'(?i)\bADS\b'), 'TPH'),
    (re.compile(r'(?i)\bSTOCK\b'), 'TPH'),
    (re.compile(r'(?i)\bPARTN\b'), 'TPH'),
    (re.compile(r'(?i)\bVERCEL\b'), 'TPH'),
    (re.compile(r'(?i)\bCABBAGE\b'), 'TPH'),
    (re.compile(r'(?i)\bXXXXXXXXXXXX8205\b'), 'TPH'),
    (re.compile(r'(?i)\bAIRCALL\b'), 'TPH'),
    (re.compile(r'(?i)\bAMZN\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bECHTRAI\b'), 'TPH'),
    (re.compile(r'(?i)\bGITHUB\b'), 'TPH'),
    (re.compile(r'(?i)\bAIRCALL\.IO\b'), 'TPH'),
    (re.compile(r'(?i)\bLLP\b'), 'TPH'),
    (re.compile(r'(?i)\bINBOUND\b'), 'NY'),
    (re.compile(r'(?i)\bATT\b'), 'TPH'),
    (re.compile(r'(?i)\bBROWSERSTACK\b'), 'TPH'),
    (re.compile(r'(?i)\bBROWSERSTACK\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bGSUITE\b'), 'TPH'),
    (re.compile(r'(?i)\bMSFT\b'), 'TPH'),
    (re.compile(r'(?i)\bANALYTIC\b'), 'TPH'),
    (re.compile(r'(?i)\bCANVA\b'), 'TPH'),
    (re.compile(r'(?i)\bASTRA\b'), 'TPH'),
    (re.compile(r'(?i)\bNGO\b'), 'TPH'),
    (re.compile(r'(?i)\bCALLRAIL\b'), 'TPH'),
    (re.compile(r'(?i)\bDNH\b'), 'TPH'),
    (re.compile(r'(?i)\bFORMSWIFT\b'), 'TPH'),
    (re.compile(r'(?i)\bFORMSWIFT\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bORIN\b'), 'DMD'),
    (re.compile(r'(?i)\bAUTOPY\b'), 'TPH'),
    (re.compile(r'(?i)\bPREPD\b'), 'TPH'),
    (re.compile(r'(?i)\bCREATIVE\b'), 'TPH'),
    (re.compile(r'(?i)\bSPECTRUM\b'), 'TPH'),
    (re.compile(r'(?i)\bFEI\b'), 'NY'),
    (re.compile(r'(?i)\bMARCO\b'), 'NY'),
    (re.compile(r'(?i)\bWORLDWIDE\b'), 'HAH'),
    (re.compile(r'(?i)\bADS7475396058\b'), 'TPH'),
    (re.compile(r'(?i)\bGSUITE_SMOAKLA\b'), 'TPH'),
    (re.compile(r'(?i)\bPASS\b'), 'NY'),
    (re.compile(r'(?i)\bPASSNY\b'), 'NY'),
    (re.compile(r'(?i)\bACROPRO\b'), 'TPH'),
    (re.compile(r'(?i)\bATLASSIAN\b'), 'TPH'),
    (re.compile(r'(?i)\bCOMETCHAT\b'), 'TPH'),
    (re.compile(r'(?i)\bSUBS\b'), 'TPH'),
    (re.compile(r'(?i)\bTRELLO\b'), 'TPH'),
    (re.compile(r'(?i)\bTRELLO\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bCLOVER\b'), 'DMD'),
    (re.compile(r'(?i)\bSMOAKLANDPORTH\b'), 'SOCAL'),
    (re.compile(r'(?i)\bFLORALS\b'), 'DMD'),
    (re.compile(r'(?i)\bLC2400\b'), 'DMD'),
    (re.compile(r'(?i)\bPMNT\b'), 'DMD'),
    (re.compile(r'(?i)\bSTONEMARK\b'), 'DMD'),
    (re.compile(r'(?i)\bALPINE\b'), 'TPH'),
    (re.compile(r'(?i)\bXEVEN\b'), 'TPH'),
    (re.compile(r'(?i)\bACHTRANS\b'), 'DMD'),
    (re.compile(r'(?i)\bIOT\b'), 'DMD'),
    (re.compile(r'(?i)\bXPRESSCPTL\b'), 'DMD'),
    (re.compile(r'(?i)\bPRIMO\b'), 'SUB'),
    (re.compile(r'(?i)\bWTH\b'), 'HAH'),
    (re.compile(r'(?i)\bGODADDY\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bLEGALZOOM\b'), 'TPH'),
    (re.compile(r'(?i)\bSQJERSEY\b'), 'TPH'),
    (re.compile(r'(?i)\bDISPENSARY\b'), 'NY'),
    (re.compile(r'(?i)\bSTATIOOAKLAND\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bSACR\b'), 'HAH 7 CA'),
    (re.compile(r'(?i)\bBILLBOAR\b'), 'TPH'),
    (re.compile(r'(?i)\bSPARKS\b'), 'TPH'),
    (re.compile(r'(?i)\bZENITH\b'), 'TPH'),
    (re.compile(r'(?i)\bECOGREENINDUSTRIES\b'), 'SUB'),
    (re.compile(r'(?i)\bECOGREENINDUSTRIES\.COM\b'), 'SUB'),
    (re.compile(r'(?i)\bSAC\.HDLGOV\b'), 'HAH'),
    (re.compile(r'(?i)\bSACRAMEN\b'), 'HAH'),
    (re.compile(r'(?i)\bHEADQUARTERS\b'), 'TPH'),
    (re.compile(r'(?i)\bROOTS\b'), 'TPH'),
    (re.compile(r'(?i)\bBEGGS\b'), 'SOCAL'),
    (re.compile(r'(?i)\bKING\b'), 'SOCAL'),
    (re.compile(r'(?i)\bBACKGROUND\b'), 'TPH'),
    (re.compile(r'(?i)\bBILLBOARD\b'), 'TPH'),
    (re.compile(r'(?i)\bZOOM\b'), 'TPH'),
    (re.compile(r'(?i)\bZOOM\.US\b'), 'TPH'),
    (re.compile(r'(?i)\bSCZZ\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bDELIGHTED\b'), 'TPH'),
    (re.compile(r'(?i)\bEVENT\b'), 'TPH'),
    (re.compile(r'(?i)\bFRONT\b'), 'TPH'),
    (re.compile(r'(?i)\bGODADDY\b'), 'TPH'),
    (re.compile(r'(?i)\bLIVE\b'), 'TPH'),
    (re.compile(r'(?i)\bSCAN\b'), 'TPH'),
    (re.compile(r'(?i)\bCONFIRMED\b'), 'DMD'),
    (re.compile(r'(?i)\bPICKUP\b'), 'DMD'),
    (re.compile(r'(?i)\bBAKESFIELD\b'), 'DMD'),
    (re.compile(r'(?i)\bPURCHASE09100001\b'), 'DMD'),
    (re.compile(r'(?i)\bSTATIOORINDA\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bADS3407900941\b'), 'TPH'),
    (re.compile(r'(?i)\bPATIENT\b'), 'DMD'),
    (re.compile(r'(?i)\bSPENCER\b'), 'DMD'),
    (re.compile(r'(?i)\b018\.00\b'), 'SSC'),
    (re.compile(r'(?i)\bCELL\b'), 'HAH'),
    (re.compile(r'(?i)\bWIDE\b'), 'HAH'),
    (re.compile(r'(?i)\bCONSULTANTS\b'), 'TPH'),
    (re.compile(r'(?i)\bSIBANNA\b'), 'TPH'),
    (re.compile(r'(?i)\bTWITCHTVDON\b'), 'TPH'),
    (re.compile(r'(?i)\bRETAIL_PAY\b'), 'DMD'),
    (re.compile(r'(?i)\bSLO\b'), 'DMD'),
    (re.compile(r'(?i)\bNET\b'), 'SOCAL'),
    (re.compile(r'(?i)\bREM\b'), 'SOCAL'),
    (re.compile(r'(?i)\bANTIOCH\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bKIM\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bTIGRANLEVONANDLA\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bANGELES\b'), 'SUB'),
    (re.compile(r'(?i)\bNETWOR\b'), 'SUB'),
    (re.compile(r'(?i)\bCONSULTING\b'), 'TPH'),
    (re.compile(r'(?i)\bDOPE\b'), 'TPH'),
    (re.compile(r'(?i)\bENTERTAINMENT\b'), 'TPH'),
    (re.compile(r'(?i)\bHIS2000461\b'), 'TPH'),
    (re.compile(r'(?i)\bSPRINGBIG\b'), 'TPH'),
    (re.compile(r'(?i)\bXXXXXXXXXXXX8630\b'), 'TPH'),
    (re.compile(r'(?i)\b08\.15\.2023\b'), 'DMD'),
    (re.compile(r'(?i)\bFHL\b'), 'DMD'),
    (re.compile(r'(?i)\bINS\.PMTS\b'), 'DMD'),
    (re.compile(r'(?i)\bNEXT\b'), 'DMD'),
    (re.compile(r'(?i)\bPREMIU\b'), 'DMD'),
    (re.compile(r'(?i)\bTIRE\b'), 'DMD'),
    (re.compile(r'(?i)\bWAVE\b'), 'DMD'),
    (re.compile(r'(?i)\bNORDHOFF\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bNUYS\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bVAN\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bDROPBOX\b'), 'TPH'),
    (re.compile(r'(?i)\bMEDIAJEL\b'), 'TPH'),
    (re.compile(r'(?i)\bSEMRUSH\b'), 'TPH'),
    (re.compile(r'(?i)\bVISTAPRINT\b'), 'TPH'),
    (re.compile(r'(?i)\bPTB\b'), 'DMD'),
    (re.compile(r'(?i)\bSYS\b'), 'DMD'),
    (re.compile(r'(?i)\bLICENSING\b'), 'NY'),
    (re.compile(r'(?i)\bSINCLAIBUTTONWILLOW\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bARIEL\b'), 'SUB'),
    (re.compile(r'(?i)\bCKO\b'), 'TPH'),
    (re.compile(r'(?i)\bCLTV8\b'), 'TPH'),
    (re.compile(r'(?i)\bDEMAND\b'), 'TPH'),
    (re.compile(r'(?i)\bONBOARD\b'), 'TPH'),
    (re.compile(r'(?i)\bSTEADY\b'), 'TPH'),
    (re.compile(r'(?i)\bXXXXXXXXXXXX7706\b'), 'TPH'),
    (re.compile(r'(?i)\bYOUNG\b'), 'DMD'),
    (re.compile(r'(?i)\bBAKED\b'), 'NY'),
    (re.compile(r'(?i)\bDISPE\b'), 'NY'),
    (re.compile(r'(?i)\bFRESHLY\b'), 'NY'),
    (re.compile(r'(?i)\bNSEW\b'), 'NY'),
    (re.compile(r'(?i)\bNYC\b'), 'NY'),
    (re.compile(r'(?i)\bTRADING\b'), 'NY'),
    (re.compile(r'(?i)\bINDIO\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bSYLMAR\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bF54C43DBBDB1\b'), 'HAH 7 CA'),
    (re.compile(r'(?i)\bARORA\b'), 'SUB'),
    (re.compile(r'(?i)\bEARTHWISE\b'), 'SUB'),
    (re.compile(r'(?i)\bMELISSA\b'), 'SUB'),
    (re.compile(r'(?i)\bPRIMPATCHARA\b'), 'SUB'),
    (re.compile(r'(?i)\bWILTON\b'), 'SUB'),
    (re.compile(r'(?i)\bAARON\b'), 'TPH'),
    (re.compile(r'(?i)\bACCELA\b'), 'TPH'),
    (re.compile(r'(?i)\bCARLOS\b'), 'TPH'),
    (re.compile(r'(?i)\bHIS2000442\b'), 'TPH'),
    (re.compile(r'(?i)\bJPS\b'), 'TPH'),
    (re.compile(r'(?i)\bKINDLE\b'), 'TPH'),
    (re.compile(r'(?i)\bOYAMASUSH\b'), 'TPH'),
    (re.compile(r'(?i)\bSENDGRID\b'), 'TPH'),
    (re.compile(r'(?i)\bSHAY\b'), 'TPH'),
    (re.compile(r'(?i)\bSHOPIFY\b'), 'TPH'),
    (re.compile(r'(?i)\bUNLTD\b'), 'TPH'),
    (re.compile(r'(?i)\bXFINITY\b'), 'TPH'),
    (re.compile(r'(?i)\bCASADOS\b'), 'DMD'),
    (re.compile(r'(?i)\bPAYROLLSYS\b'), 'DMD'),
    (re.compile(r'(?i)\bROCKET\b'), 'DMD'),
    (re.compile(r'(?i)\bWENDI\b'), 'DMD'),
    (re.compile(r'(?i)\bGUARDIAN\b'), 'NY'),
    (re.compile(r'(?i)\bISLAMIC\b'), 'SOCAL'),
    (re.compile(r'(?i)\bBANYAN\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bSTATIOCASTAIC\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bOBATA\b'), 'SUB'),
    (re.compile(r'(?i)\bESCROW\b'), 'HAH'),
    (re.compile(r'(?i)\bVENTURE\b'), 'HAH'),
    (re.compile(r'(?i)\bX0001\b'), 'HAH'),
    (re.compile(r'(?i)\bX1182\b'), 'HAH'),
    (re.compile(r'(?i)\bANA\b'), 'TPH'),
    (re.compile(r'(?i)\bASSETS\b'), 'TPH'),
    (re.compile(r'(?i)\bFAX\b'), 'TPH'),
    (re.compile(r'(?i)\bFIXED\b'), 'TPH'),
    (re.compile(r'(?i)\bGILMOR\b'), 'TPH'),
    (re.compile(r'(?i)\bHDN\b'), 'TPH'),
    (re.compile(r'(?i)\bIMPROVEMENTS\b'), 'TPH'),
    (re.compile(r'(?i)\bJATO\b'), 'TPH'),
    (re.compile(r'(?i)\bLEASEHOLD\b'), 'TPH'),
    (re.compile(r'(?i)\bSAIGONHOU\b'), 'TPH'),
    (re.compile(r'(?i)\bTARGET\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bTIME\b'), 'TPH'),
    (re.compile(r'(?i)\bTST\b'), 'TPH'),
    (re.compile(r'(?i)\bYELLOWIMAGES\b'), 'TPH'),
    (re.compile(r'(?i)\bYELLOWIMAGES\.COM\b'), 'TPH'),
    (re.compile(r'(?i)\bANT\b'), 'DMD'),
    (re.compile(r'(?i)\bBARR\b'), 'DMD'),
    (re.compile(r'(?i)\bBLV\b'), 'DMD'),
    (re.compile(r'(?i)\bFINANCING\b'), 'DMD'),
    (re.compile(r'(?i)\bGOMES\b'), 'DMD'),
    (re.compile(r'(?i)\bGUYS\b'), 'DMD'),
    (re.compile(r'(?i)\bIRMA\b'), 'DMD'),
    (re.compile(r'(?i)\bLEONIDAS\b'), 'DMD'),
    (re.compile(r'(?i)\bLTD\b'), 'DMD'),
    (re.compile(r'(?i)\bMICHAEL\b'), 'DMD'),
    (re.compile(r'(?i)\bPFS\b'), 'DMD'),
    (re.compile(r'(?i)\bPROCESSING\b'), 'DMD'),
    (re.compile(r'(?i)\bBILL_PAY\b'), 'NY'),
    (re.compile(r'(?i)\bCASE\b'), 'NY'),
    (re.compile(r'(?i)\bEXIT\b'), 'NY'),
    (re.compile(r'(?i)\bEXOTIC\b'), 'NY'),
    (re.compile(r'(?i)\bNUCLEUS\b'), 'NY'),
    (re.compile(r'(?i)\bTWENTY8GRAMZ\b'), 'NY'),
    (re.compile(r'(?i)\bARCBUTTONWILLOW\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bARTESIA\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bFINANCIALS\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bHILIFE\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bWEBSTAURANT\b'), 'SUB'),
    (re.compile(r'(?i)\b3QHK\b'), 'HAH'),
    (re.compile(r'(?i)\bPAYMENTS\b'), 'HAH'),
    (re.compile(r'(?i)\bSAEFONG\b'), 'HAH'),
    (re.compile(r'(?i)\bTEL\b'), 'HAH'),
    (re.compile(r'(?i)\bEUNOIA\b'), 'TPH'),
    (re.compile(r'(?i)\bFORMATION\b'), 'TPH'),
    (re.compile(r'(?i)\bGROWTH\b'), 'TPH'),
    (re.compile(r'(?i)\bHONOR\b'), 'TPH'),
    (re.compile(r'(?i)\bIPS\b'), 'TPH'),
    (re.compile(r'(?i)\bLZC\b'), 'TPH'),
    (re.compile(r'(?i)\bMETRO\b'), 'TPH'),
    (re.compile(r'(?i)\bMICROSOFT\b'), 'TPH'),
    (re.compile(r'(?i)\bPUBLISHING\b'), 'TPH'),
    (re.compile(r'(?i)\bTENSTRIKE\b'), 'TPH'),
    (re.compile(r'(?i)\bTRINITY\b'), 'TPH'),
    (re.compile(r'(?i)\bWINGSTOP\b'), 'TPH'),
    (re.compile(r'(?i)\bWPY\b'), 'TPH'),
    (re.compile(r'(?i)\bBILLPAY\b'), 'DMD'),
    (re.compile(r'(?i)\bBLOOM\b'), 'DMD'),
    (re.compile(r'(?i)\bBRIANA\b'), 'DMD'),
    (re.compile(r'(?i)\bCORNER\b'), 'DMD'),
    (re.compile(r'(?i)\bDEPOSITS\b'), 'DMD'),
    (re.compile(r'(?i)\bDIAZ\b'), 'DMD'),
    (re.compile(r'(?i)\bEXTRACT\b'), 'DMD'),
    (re.compile(r'(?i)\bFOUR\b'), 'DMD'),
    (re.compile(r'(?i)\bFROOT\b'), 'DMD'),
    (re.compile(r'(?i)\bHENRIQUE\b'), 'DMD'),
    (re.compile(r'(?i)\bJAVEN\b'), 'DMD'),
    (re.compile(r'(?i)\bJOHNSON\b'), 'DMD'),
    (re.compile(r'(?i)\bKANDY\b'), 'DMD'),
    (re.compile(r'(?i)\bMARIA\b'), 'DMD'),
    (re.compile(r'(?i)\bMEGANS\b'), 'DMD'),
    (re.compile(r'(?i)\bMORALES\b'), 'DMD'),
    (re.compile(r'(?i)\bNICE\b'), 'DMD'),
    (re.compile(r'(?i)\bNORTHBAY\b'), 'DMD'),
    (re.compile(r'(?i)\bPAULO\b'), 'DMD'),
    (re.compile(r'(?i)\bPIN\b'), 'DMD'),
    (re.compile(r'(?i)\bPINHEIRO\b'), 'DMD'),
    (re.compile(r'(?i)\bRETA\b'), 'DMD'),
    (re.compile(r'(?i)\bSAECHAO\b'), 'DMD'),
    (re.compile(r'(?i)\bSHENZHEN\b'), 'DMD'),
    (re.compile(r'(?i)\bWELLNES\b'), 'DMD'),
    (re.compile(r'(?i)\bYENI\b'), 'DMD'),
    (re.compile(r'(?i)\bZIPPY\b'), 'DMD'),
    (re.compile(r'(?i)\bMEDIA\b'), 'TPH'),
    (re.compile(r'(?i)\bSERIAL\b'), 'HAH'),
    (re.compile(r'(?i)\bTELEPHONE\b'), 'TPH'),
    (re.compile(r'(?i)\bSCHG\b'), 'HAH'),
    (re.compile(r'(?i)\bCASHLESS\b'), 'DMD'),
    (re.compile(r'(?i)\bBATCH\b'), 'HAH'),
    (re.compile(r'(?i)\bOUTDOOR\b'), 'TPH'),
    (re.compile(r'(?i)\bLEGAL\b'), 'TPH'),
    (re.compile(r'(?i)\bMARKETING\b'), 'TPH'),
    (re.compile(r'(?i)\bSOCAL\b'), 'SOCAL'),
    (re.compile(r'(?i)\bCLOUD\b'), 'TPH'),
    (re.compile(r'(?i)\bTWILIO\b'), 'TPH'),
    (re.compile(r'(?i)\bDEPOS\b'), 'HAH'),
    (re.compile(r'(?i)\bHISIG\b'), 'TPH'),
    (re.compile(r'(?i)\bOTC\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bPCS\b'), 'TPH'),
    (re.compile(r'(?i)\bINTERNAL\b'), 'DMD'),
    (re.compile(r'(?i)\bONLY\b'), 'DMD'),
    (re.compile(r'(?i)\bHARBOR\b'), 'TPH'),
    (re.compile(r'(?i)\bDOORDASH\b'), 'TPH'),
    (re.compile(r'(?i)\bREBILL\b'), 'NY'),
    (re.compile(r'(?i)\bPROPERTY\b'), 'DMD'),
    (re.compile(r'(?i)\bKUSHMEN\b'), 'DMD'),
    (re.compile(r'(?i)\bDINING\b'), 'TPH'),
    (re.compile(r'(?i)\bRAJ\b'), 'HAH'),
    (re.compile(r'(?i)\bSPIRIT\b'), 'TPH'),
    (re.compile(r'(?i)\b08\.01\.2023\b'), 'DMD'),
    (re.compile(r'(?i)\bDEEP\b'), 'TPH'),
    (re.compile(r'(?i)\bCHANGER\b'), 'DMD'),
    (re.compile(r'(?i)\bLONG\b'), 'DMD'),
    (re.compile(r'(?i)\bARMOR\b'), 'HAH'),
    (re.compile(r'(?i)\bAIRL\b'), 'TPH'),
    (re.compile(r'(?i)\bINTERCOMPANY\b'), 'TPH'),
    (re.compile(r'(?i)\bDISTRO\b'), 'DMD'),
    (re.compile(r'(?i)\bCOMM\b'), 'TPH'),
    (re.compile(r'(?i)\bFRONTIER\b'), 'TPH'),
    (re.compile(r'(?i)\bINVOICES\b'), 'DMD'),
    (re.compile(r'(?i)\bAIRLINES\b'), 'TPH'),
    (re.compile(r'(?i)\bSMO\b'), 'SSC'),
    (re.compile(r'(?i)\bHIGHLAND\b'), 'DMD'),
    (re.compile(r'(?i)\bYORK\b'), 'NY'),
    (re.compile(r'(?i)\bADT\b'), 'SUB'),
    (re.compile(r'(?i)\bBAT\b'), 'HAH 7 CA'),
    (re.compile(r'(?i)\bBUSINESS\b'), 'TPH'),
    (re.compile(r'(?i)\bMOMENTUM\b'), 'DMD'),
    (re.compile(r'(?i)\bWORLD\b'), 'HAH'),
    (re.compile(r'(?i)\bMAITLANDM\b'), 'TPH'),
    (re.compile(r'(?i)\bCONSULTANT\b'), 'TPH'),
    (re.compile(r'(?i)\bLESS\b'), 'DMD'),
    (re.compile(r'(?i)\bDAYS\b'), 'NY'),
    (re.compile(r'(?i)\bINDUS\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bCHEVLEBEC\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bEMPYREALENTERPRI\b'), 'DMD'),
    (re.compile(r'(?i)\b08\.14\.2023\b'), 'DMD'),
    (re.compile(r'(?i)\bITEM\b'), 'DMD'),
    (re.compile(r'(?i)\bSTATION\b'), 'DMD'),
    (re.compile(r'(?i)\bC043488NEWARK\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bCHEVWESTLEY\b'), 'DISTRIBUTION'),
    (re.compile(r'(?i)\bCONTRACTOR\b'), 'TPH'),
    (re.compile(r'(?i)\bHIS2000444\b'), 'TPH'),
    (re.compile(r'(?i)\bAUTOMOT\b'), 'DMD'),
    (re.compile(r'(?i)\bIMPORT\b'), 'DMD'),
]


_SOURCE_COLS: List[str] = [
    "bank_account", "subentity", "bank_cc_num", "week_num",
    "payee_vendor",           
    "cf_account",             # idem
    "dashboard_1",            # idem
    "budget_owner",           # idem
    "qbo_account", "qbo_subaccount",
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
