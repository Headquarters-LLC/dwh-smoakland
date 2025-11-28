"""
Auto-generated Payee/Vendor rules engine.
Strategy: Iterate deterministic regex rules (precision >= 0.90, coverage >= 5, ordered by precision then coverage).
"""

import re
import pandas as pd
from typing import List, Tuple

_SOURCE_COLS = [
    "bank_cc_num", "description", "extended_description",
]

_RULEBOOK_NAME = "payee_vendor"
_RULEBOOK_VERSION = "2025.11.21"
_POSTPROCESS_TAG = f"{_RULEBOOK_NAME}@{_RULEBOOK_VERSION}#post-check-amount"
_POSTPROCESS_SOURCE = "postprocess"
_CHECK_PATTERN = re.compile(r"(?i)\bCHECK\b")


# --- Rules (pattern, payee, rule_id optional) --------------------------------
_RULES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bBLAZE\b'), 'BLAZE'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*CURRENCY\s+AND\s+COIN\s+DEPOSITED'), 'CURRENCY AND COIN DEPOSITED'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bDOORDASH\b'), 'DOORDASH'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bOSS\b'), 'OSS'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bDISPOJOY\b'), 'DISPOJOY'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bCHASE\b'), 'CC'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bERACTOLL\b'), 'ERACTOLL'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bAPSMOAKLAND\b'), 'AEROPAY'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*MATRIX\s+TRUST'), 'MATRIX TRUST'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*QUIK\s+STOP'), 'QUIK STOP'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bNABIFIVE\s+LLC\b'), 'NABIFIVE LLC'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bACCTVERIFY\b'), 'ACCTVERIFY'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bTETRA\s+HOUSE,\s*CO\.\b'), 'TETRA HOUSE, CO.'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bHAPPY\s+DAYS\b'), 'HAPPY DAYS DISPENSARY'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bFOREST\s+PARK\b'), 'FOREST PARK'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bALIBABA\b'), 'ALIBABA'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bNISSAN\b'), 'NISSAN'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bAUTOZONE\b'), 'AUTOZONE'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bDOF\b'), 'NY DOF'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bFRESHLY\s+BAKED\b'), 'FRESHLY BAKED'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bCONED\b'), 'EDISON'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bGEICO\b'), 'GEICO'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bHEADSET\s+INC\b'), 'HEADSET INC'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bEZPASS\b'), 'EZ-PASS'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bAMEX\b'), 'CC'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bNYS\s+HEMP\s+PROGRAM\b'), 'NYS HEMP PROGRAM'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bLEAFYWONDERSLLC\b'), 'LEAFYWONDERSLLC'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bPARKING\b'), 'PARKING'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bSHARE\s+DRAFT\s+CLEARING\b'), 'PAYNW'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bCASE\s+MANAGEMENT\b'), 'CASE MANAGEMENT'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bNATIONAL\s+GRID\b'), 'NATIONAL GRID'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bSTARLIFE\b'), 'STAR LIFE RETAIL'),
    (re.compile(r'(?i)DESCRIPTION:[^|]*\bUNION\b'), 'UNION'),
#---TextFSM rules below---------------------------------
    (re.compile(r'(?i)\b(?:ACCELA\ WEB)\b'), 'ACCELA WEB'),
    (re.compile(r'(?i)\b(?:ACME\ FIRE\ EXTINGUISHER\ CO)\b'), 'ACME FIRE EXTINGUISHER CO'),
    (re.compile(r'(?i)\b(?:ADCHEM\ LLC)\b'), 'ADCHEM LLC'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ADOBE|STOCK|ADOBE\ INC\.|CREATIVE\ CLOUD|ACROPRO\ SUBS|ADOBE\ 4085366000\ CA\ NULL\ XXXXXXXXXXXX8630|ADOBE\ SAN\ JOSE\ CA\ NULL\ XXXXXXXXXXXX7706|800\-833\-6687)(?![A-Z0-9])'), 'ADOBE'),
    (re.compile(r'(?i)\b(?:ADT|090116278)\b'), 'ADT'),
    (re.compile(r'(?i)\b(?:AD\ ASTRA\ LAW\ GROUP\ LLP\ SAN\ FRANCISCO\ CA|AD\ ASTRA\ LAW\ GROUP\ LLP)\b'), 'AD ASTRA LAW GROUP'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ACH\ \-\ APSMOAKLAND\ OAK|AEROPAY|AEROPAY\ AEROPAY|AEROPAY\-AEROPAY|PURCHASE\ SMOAKLAND\ \-\ PH\ APSMOAKLAND\ \-\ P|SMOAKLAND\ OAKLAN\ PURCHASE\ APSMOAKLAND\ OAK|\bAEROPAY\b|ACH[, \- ]?AEROPAY|(?:AEROPAY.*APS?MOAKLAND|APS?MOAKLAND.*AEROPAY))(?![A-Z0-9])'), 'AEROPAY'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:AIRCALL\.IO|CKO)(?![A-Z0-9])'), 'AIRCALL'),
    (re.compile(r'(?i)\b(?:ALAMEDA\ IMPORT\ AUTOMOT)\b'), 'ALAMEDA IMPORT'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ALAMO\ RENT\-A\-CAR|ALAMO\ RENT\-A\-C043488NEWARK\ NJ)(?![A-Z0-9])'), 'ALAMO RENT-A-CAR'),
    (re.compile(r'(?i)\b(?:ALAMO\ TOLL\ MESA\ AZ|ALAMO\ TOLL)\b'), 'ALAMO TOLL'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CITY\ OF\ OAKLAND\-ALARM\ PRO)(?![A-Z0-9])'), 'ALARM PRO'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ALIBABA\.COM\ HONG\ KONCAUSEWAY\ BAY\ HK)(?![A-Z0-9])'), 'ALIBABA'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ALIBABA\.COM)(?![A-Z0-9])'), 'ALIBABA.COM'),
    (re.compile(r'(?i)\b(?:ALKHEMIST\ DM\ LLC)\b'), 'ALKHEMIST DM LLC'),
    (re.compile(r'(?i)\b(?:ALPINE\ IQ)\b'), 'ALPINE IQ'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:AMZN\ MKTP\ US|AMAZON\ MKTPL|AMAZON\.COM|AMAZON\ MKTPLACE\ PMTS|AMAZON\ MARK|ANNUAL\ MEMBERSHIP\ FEE|AMAZON\ RETA)(?![A-Z0-9])'), 'AMAZON'),
    (re.compile(r'(?i)\b(?:AMAZON\ WEB\ SERVICES)\b'), 'AMAZON WEB SERVICES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:APPFOLIO|INC\.\ F\ WEB\ PMTS|INC\.\ F\-WEB\ PMTS)(?![A-Z0-9])'), 'APPFOLIO, INC'),
    (re.compile(r'(?i)\b(?:ARCO|83844WESTLEY\ PETROL|83844WESTLEY\ PEWESTLEY\ CA|42483\ AMPM\ ARCBUTTONWILLOW\ CA|07127ARCO)\b'), 'ARCO'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ARMORED\ CAR\ PICKUP\ CONFIRMED|GREENBAX\ MARKETP\-ARMOREDCAR|ARMORED\ CAR|ARMORED\ CARRIER|GBX\ \-\ DEBIT)(?![A-Z0-9])'), 'ARMORED CAR'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:XPRESSCPTL/ATLAS\ ACHTRANS|ACHTRANS|INS\.PREM|ATLAS\ GENERAL\ INS\.PREM)(?![A-Z0-9])'), 'ATLAS'),
    (re.compile(r'(?i)\b(?:BUS\ PHONE\ PMT|ATT)\b'), 'ATT'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:AVIS\ RENT\-A\-CAR|AVIS\.COM\ PREPAY)(?![A-Z0-9])'), 'AVIS RENT-A-CAR'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CCD\ \-\ PO)(?![A-Z0-9])'), 'B2B'),
    (re.compile(r'(?i)\b(?:BAY\ ALARM\ COMPANY)\b'), 'BAY ALARM COMPANY'),
    (re.compile(r'(?i)\b(?:BELCOSTA\ LABS\ LO\ SALE|BELCOSTA\ LABS\ LO|PAYMENT\ BELCOSTA\ LABS\ LONG\ BEACH\ LLC|BELCOSTA\ LABS\ LO\ SALE\ 250418)\b'), 'BELCOSTA LABS'),
    (re.compile(r'(?i)\b(?:S\ VALERO)\b'), 'BILL\'S VALERO'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:BLAZE\ SOLUTIONS|BLAZE\ SOLUTIONS\ SALE|BLAZE\ SOLUTIONS\-SALE)(?![A-Z0-9])'), 'BLAZE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:BROWSERSTACK\.COM)(?![A-Z0-9])'), 'BROWSERSTACK'),
    (re.compile(r'(?i)\b(?:BUTTONWILLOW\ SINCLAIBUTTONWILLOW\ CA)\b'), 'BUTTONWILLOW'),
    (re.compile(r'(?i)\b(?:BUTTONWILLOW\ SINCLAIR)\b'), 'BUTTONWILLOW SINCLAIR'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:B\ \&\ J\ AUTO\ CENTER)(?![A-Z0-9])'), 'B & J AUTO CENTER'),
    (re.compile(r'(?i)\b(?:CALIFORNIA\ EXTRACTION)\b'), 'CALIFORNIA EXTRACTION'),
    (re.compile(r'(?i)\b(?:CALIFORNIA\ STOP)\b'), 'CALIFORNIA STOP'),
    (re.compile(r'(?i)\b(?:CALLRAIL\ INC)\b'), 'CALLRAIL INC'),
    (re.compile(r'(?i)\b(?:CANOPY\ JERSEY\ CITY)\b'), 'CANOPY JERSEY CITY'),
    (re.compile(r'(?i)\b(?:CANVA)\b'), 'CANVA'),
    (re.compile(r'(?i)\b(?:CAPITAL\ LIVE\ SCAN)\b'), 'CAPITAL LIVE SCAN'),
    (re.compile(r'(?i)\b(?:NORTH\ BAY\ CU|121182860000005|DEPOSIT\ NORTH\ BAY\ CREDIT\ UNION)\b'), 'CASHLESS ATM'),
    (re.compile(r'(?i)\b(?:CA\ DMV\ FEE)\b'), 'CA DMV'),
    (re.compile(r'(?i)\b(?:CA\ SECRETARY\ OF\ STATE)\b'), 'CA SECRETARY OF STATE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ONLINE\ PAYMENT\ \-\ THANK\ YOU|CHASE\ CREDIT\ CRD\-EPAY|THANK\ YOU|ONLINE\ PAYMENT|PAYMENT\ THANK\ YOU)(?![A-Z0-9])'), 'CC'),
    (re.compile(r'(?i)\b(?:CAPITAL\ ONE\ MOBILE\ PMT)\b'), 'CC 8202'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CA\ DEPT\ TAX\ FEE|CDTFA(?:\ EPMT)?)(?![A-Z0-9])'), 'CDTFA'),
    (re.compile(r'(?i)\b(?:CHEFSTORE\ 7567\ OAKLAND\ CA|CHEFSTORE\ 7567)\b'), 'CHEFSTORE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CHEVRON\ 0097407|CHEVRON\ 0205456|CHEVRON\ 0097407\ ORIN|CHEVRON\ 0378784|CHEVRON\ 0378784/CHEVLEBEC\ CA|CHEVRON\ 0097407\ ORINDA\ CA|CHEVRON\ 0205456/CHEVWESTLEY\ CA|CHEVRON\ 0371126|CHEVRON\ 0090289|CHEVRON\ 0097407/CHEVORINDA\ CA|CHEVRON\ 0092513|CHEVRON\ 0090338|CHEVRON)(?![A-Z0-9])'), 'CHEVRON'),
    (re.compile(r'(?i)\b(?:CINTAS\ CORP)\b'), 'CINTAS CORP'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EXTERNAL\ WITHDRAWAL\ CITY\ OF\ SACRAMEN\ \-\ SAC\.HDLGOV|CITYOFSAC_BIZTAXONLINE|EXTERNAL\ WITHDRAWAL\ CITY\ OF\ SACRAMEN\ \-)(?![A-Z0-9])'), 'CITY OF SACRAMENTO'),
    (re.compile(r'(?i)\b(?:COMCAST\ CALIFORNIA|COMCAST\ BUSINESS)\b'), 'COMCAST'),
    (re.compile(r'(?i)\b(?:COMETCHAT)\b'), 'COMETCHAT'),
    (re.compile(r'(?i)\b(?:CONED\ BILL\ PAYMENT|FSI)\b'), 'CONED'),
    (re.compile(r'(?i)\b(?:COSTCO|COSTCO\ WHSE|BUS\ DELIV\ 823|0663\ CO|0118\ SA|WWW\ COSTCO\ COM)\b'), 'COSTCO'),
    (re.compile(r'(?i)\b(?:CUSTOM\ CONES\ USA)\b'), 'CUSTOM CONES USA'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:DELIVERMD\-TRANSFER\ TOTAL\ DEPOSIT)(?![A-Z0-9])'), 'DAMA FINANCIAL 2370'),
    (re.compile(r'(?i)\b(?:DCC)\b'), 'DCC'),
    (re.compile(r'(?i)\b(?:DEEP\ ROOTS\ PARTN\ SALE)\b'), 'DEEP ROOTS'),
    (re.compile(r'(?i)\b(?:DELIGHTED)\b'), 'DELIGHTED, LLC'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:DOMESTIC\ WIRE\ WITHDRAWAL\ WIRE\ TO\ DELIVERMD|ACCOUNT_TO_ACCOUNT\ DELIVERMD\ CHECKING\ 28\-03\-01\-000073|TRANSFER\ DELIVERMD)(?![A-Z0-9])'), 'DELIVERMD'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:DIXON\ GAS\ \&\ SHOP)(?![A-Z0-9])'), 'DIXON GAS & SHOP'),
    (re.compile(r'(?i)\b(?:STATE\ OF\ CALIF\ DMV\ INT\ SC)\b'), 'DMV'),
    (re.compile(r'(?i)\b(?:DINING|DD\ DOORDASH\ MAITLANDM|DD\ DOORDASH\ OYAMASUSH|DD\ DOORDASH\ SAIGONHOU)\b'), 'DOORDASH'),
    (re.compile(r'(?i)\b(?:DOPE\ MARKETING)\b'), 'DOPE MARKETING'),
    (re.compile(r'(?i)\b(?:PAYMENT\ DOUG\ NGO)\b'), 'DOUG NGO'),
    (re.compile(r'(?i)\b(?:DROPBOX\ FAX\ ONE\ TIME)\b'), 'DROPBOX'),
    (re.compile(r'(?i)\b(?:EARTHWISE\ PACKAGING)\b'), 'EARTHWISE PACKAGING'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:MISC\.\ FEES|DEBIT\ ADJUSTMENT\ 00000000000|814)(?![A-Z0-9])'), 'EASTWEST BANK'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EBMUD\-REMITCTR/EZ\-PAY|WATER\ BILL|EBMUD\ WATER\ BILL|UTILITY\ PM|EBMUD\ UTILITY\ PM)(?![A-Z0-9])'), 'EBMUD'),
    (re.compile(r'(?i)\b(?:PAYMENT\ ECHTRAI\ LLC)\b'), 'ECHTRAI LLC'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ECOGREENINDUSTRIES\.COM)(?![A-Z0-9])'), 'ECOGREENINDUSTRIES'),
    (re.compile(r'(?i)\b(?:SO\ CALIFORNIA\ EDISON)\b'), 'EDISON'),
    (re.compile(r'(?i)\b(?:ELITESECURI)\b'), 'ELITESECURITY'),
    (re.compile(r'(?i)\b(?:EMPYREALENTERPRI\ PURCHASE|PURCHASE09100001|PURCHASE\ 091000010000001|EMPYREALENTERPRI|DELIVERMD\ SMOAKLAND|PAYMENT\ EMPYREAL\ ENTERPRISES)\b'), 'EMPYREAL ENTERPRISES, LLC'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ENTERPRISE\ 079259SAN\ LEANDRO\ CA|ENTERPRISE\ RENT\-A\-CAR)(?![A-Z0-9])'), 'ENTERPRISE RENT-A-CAR'),
    (re.compile(r'(?i)\b(?:EPOD\ HOLDINGS\ LLC)\b'), 'EPOD HOLDINGS LLC'),
    (re.compile(r'(?i)\b(?:EUNOIA\ CONSULTING\ GROUP\ LLC)\b'), 'EUNOIA CONSULTING GROUP LLC'),
    (re.compile(r'(?i)\b(?:EVERON\ LLC)\b'), 'EVERON LLC'),
    (re.compile(r'(?i)\b(?:FR\ ACC\ 08420003447|INDIVIDUAL\ AUTOMATIC\ TRANSFER\ CREDIT)\b'), 'EWB 3447'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:E\-Z|PASSNY\ REBILL)(?![A-Z0-9])'), 'E-Z*PASS'),
    (re.compile(r'(?i)\b(?:FAMILY\ FLORALS)\b'), 'FAMILY FLORALS'),
    (re.compile(r'(?i)\b(?:S\ FRIDGE|FARMER)\b'), 'FARMER\'S FRIDGE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:FASTRAK\ CSC|FASTRAK\ CSC\ SAN\ FRANCISCO\ CA|FASTRAK|BRIDGE\ TOLLS?|BAY\ AREA\ TOLL)(?![A-Z0-9])'), 'FASTRAK'),
    (re.compile(r'(?i)\b(?:FHL\ WEB\ INC\ ACH\ PMT|ACH\ PMT)\b'), 'FHL WEB INC'),
    (re.compile(r'(?i)\b(?:FLOR\ X\ INC\ GRUPO\ SMOAKLAND)\b'), 'FLOR X'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:FORMSWIFT\.COM/CHARGE)(?![A-Z0-9])'), 'FORMSWIFT'),
    (re.compile(r'(?i)\b(?:FRONTIER\ COMM\ CORP\ WEB)\b'), 'FRONTIER COMM CORP WEB'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:FRONT\ GROWTH\-2)(?![A-Z0-9])'), 'FRONT GROWTH'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:FRONT\ PRIME\-1)(?![A-Z0-9])'), 'FRONT PRIME'),
    (re.compile(r'(?i)\b(?:GAIN\ FCU\ DAILY\ PAYM)\b'), 'GAIN FCU'),
    (re.compile(r'(?i)\b(?:GAS\ 4\ LESS)\b'), 'GAS 4 LESS'),
    (re.compile(r'(?i)\b(?:GAWFCO\ WESTLEY\ 00000WESTLEY\ CA)\b'), 'GAWFCO WESTLEY'),
    (re.compile(r'(?i)\b(?:GHOST\ MGMT|GHOST\ MANAGEMENT\ GROUP|GHOST\ MANAGEMENT\ GROUP\ LL)\b'), 'GHOST MGMT'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:GITHUB|GITHUB\ INC\.)(?![A-Z0-9])'), 'GITHUB'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:GODADDY\.COM)(?![A-Z0-9])'), 'GODADDY.COM'),
    (re.compile(r'(?i)\b(?:GOOGLE\ ADS7475396058|ADS3407900941)\b'), 'GOOGLE ADS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:GSUITE_SMOAKLA|GSUITE\ SMOAKLAN|GSUITE_SMOAKLA\ 650\-253\-0000\ CA)(?![A-Z0-9])'), 'GOOGLE GSUITE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:GAS\ \&\ FOOD)(?![A-Z0-9])'), 'GO! GAS & FOOD'),
    (re.compile(r'(?i)\b(?:GRAINGER)\b'), 'GRAINGER'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:INC\ CHECKING\ \-\ 30\-03\-01\-002866|ACCOUNT_TO_ACCOUNT\ GREENBAX\ MARKETPLACE|RETURN\ ITEM\ FEE\ FOR\ CHECK)(?![A-Z0-9])'), 'GREENBAX MARKETPLACE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ACCTVERIFY|CHECK\ SERIAL|EXTERNAL\ WITHDRAWAL\ GUSTO\ \-\ TRANSFER\ 6|EXTERNAL\ WITHDRAWAL\ SMOAKLAND\ \-\ SACR\ 276296\ \-\ PURCHASE|GBX\ ACH\ 081023\-GUSTO|GBX\ ACH\-GUSTO|GUSTO|GUSTO\ CORPORATE\ FEE|GUSTO\ CORPORATE\-FEE|GUSTO\ PAYMENT|GUSTO\ PAYROLL\ SERVICE|GUSTO\ TRANSFER|GUSTO\-PAYMENT|PAYMENT\ ANGELES\ PEREZ|PAYMENT\ ARIEL\ LOPEZ|PAYMENT\ DEREK\ OBATA|PAYMENT\ GUSTO|PAYMENT\ MELISSA\ MUNOZ|PAYMENT\ STEPHANIE\ MENDOZA|\bGUSTO\b|GUSTO[- ]PAYMENT|GUSTO\ CORPORATE[- ]FEE)(?![A-Z0-9])'), 'GUSTO'),
    (re.compile(r'(?i)\b(?:HAH\ 7\ CA\ LLC|HAH\ 7\ CA\ LLC\ DMD\ NBCU|DMD\ NBCU)\b'), 'HAH'),
    (re.compile(r'(?i)\b(?:HAPPY\ CABBAGE\ ANALYTIC|HAPPY\ CABBAGE\ ANA)\b'), 'HAPPY CABBAGE'),
    (re.compile(r'(?i)\b(?:HARBOR\ HR\ LLC\ SALE)\b'), 'HARBOR HR LLC'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:HARRENS\ LAB\ INC\.)(?![A-Z0-9])'), 'HARRENS LAB INC.'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:76\ \-\ HEGENBERGER\ FOOD\ AND|76\ \-\ HEGENBERGER\ FOOD)(?![A-Z0-9])'), 'HEGENBERGER'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:HOLIDAY\ INN\ EXPRESS\ \&AMP)(?![A-Z0-9])'), 'HOLIDAY INN'),
    (re.compile(r'(?i)\b(?:HONEYBUCKET\ 2539041324|2539041324|HONEY\ BUCKET\ PAYMENT)\b'), 'HONEY BUCKET'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:HUMANA|INS\ PYMT|INC\.\ INS\ PYMT|INC\.\-INS\ PYMT)(?![A-Z0-9])'), 'HUMANA, INC.'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:HUMAN\ INTE|HUMAN\ INTEREST\ HUMAN\ INTE|HUMAN\ INTEREST\-HUMAN\ INTE|HUMAN\ INTEREST)(?![A-Z0-9])'), 'HUMAN INTEREST'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:H\ \&\ L\ PRIVATE\ \-\ I|CCD\ \-\ DELIVERMD\ CHECKING\ H\ \&\ L\ PRIVATE\ \-\ INVO|CCD\ \-\ SUBLIME\ MACHINING\ H\ \&\ L\ PRIVATE\ \-\ INVO|CCD\ \-\ SUBLIME\ MACHINING\ H\ \&\ L\ PRIVATE\ \-\ INVOI)(?![A-Z0-9])'), 'H & L PRIVATE SECURITY'),
    (re.compile(r'(?i)\b(?:IBUDDY\ INC)\b'), 'IBUDDY INC'),
    (re.compile(r'(?i)\b(?:INFINITY\ KAT)\b'), 'INFINITY'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:CCD\ \-\ DELIVERMD\ CHECKING\ INTELLIWORX\ PH\ \-\ INV|PAYMENT\ INTELLIWORX\ PH|INTELLIWORX\ PH\ \-|CCD\ \-\ DELIVERMD\ CHECKING\ INTELLIWORX\ PH\ \-\ IN)(?![A-Z0-9])'), 'INTELLIWORX PH'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:JOINHOMEBASE\.COM)(?![A-Z0-9])'), 'JOINHOMEBASE'),
    (re.compile(r'(?i)\b(?:JOINTCOMMERCE\ LLC)\b'), 'JOINTCOMMERCE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:JPS\ INC\.)(?![A-Z0-9])'), 'JPS INC.'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:KAISER\ GROUP\ DUE\ INTERNET|EXTERNAL\ WITHDRAWAL\ KAISER\ GROUP\ DUE\ TELECHK\ 800\-697\-9263\ \-\ INTERNET|KAISER\ GROUP\ DUE\-INTERNET|KAISER\ GROUP\ DUE|PPD|PAYMENT\ KAISER\ PERMANENTE)(?![A-Z0-9])'), 'KAISER'),
    (re.compile(r'(?i)\b(?:MAINTENANCE\ SERVICE\ CHARGE\ MONTHLY\ SERVICE\ FEE|MAINTENANCE\ SERVICE\ CHARGE\ MONTHLY\ SERVI|DOMESTIC\ WIRE\ DEPOSIT\ FEE\ INCOMING\ WIRE\ FEE)\b'), 'KEYPOINT CREDIT UNION'),
    (re.compile(r'(?i)\b(?:KINDLE\ UNLTD)\b'), 'KINDLE UNLTD'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:KLAVIYO\ INC\.\ SOFTWARE)(?![A-Z0-9])'), 'KLAVIYO'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:KUSHMEN\ \&\ BAKEFIELDS)(?![A-Z0-9])'), 'KUSHMEN & BAKEFIELDS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:KUSHMEN\ \&\ BAKESFIELD\ ENTERPRISES)(?![A-Z0-9])'), 'KUSHMEN & BAKESFIELD ENTERPRISES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ZIPPY\ MART\-KWIK\ SERV\ B)(?![A-Z0-9])'), 'KWIK SERV'),
    (re.compile(r'(?i)\b(?:LAFAYETTE\ RES\ METER\ PARK)\b'), 'LAFAYETTE RES METER PARK'),
    (re.compile(r'(?i)\b(?:LAZ\ PKG\ OAKLAND)\b'), 'LAZ PKG OAKLAND'),
    (re.compile(r'(?i)\b(?:LB\ MEDIA\ GROUP|LB\ MEDIA\ GROUP\ LLC|LLC)\b'), 'LB MEDIA GROUP LLC'),
    (re.compile(r'(?i)\b(?:LC2400\ LLC)\b'), 'LC 2400 (CLOVER VALLEY)'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EXTERNAL\ DEPOSIT\ LEDGER\ GREEN\ \-\ DAILY\ P|LEDGER\ GREEN|DEPOSIT\ LEDGER\ GREEN|EXTERNAL\ DEPOSIT\ LEDGER\ GREEN\ \-\ DAILY\ PAYM\ LG|LEDGER\ GREEN\ DAILY\ PAYM|COMMISSION)(?![A-Z0-9])'), 'LEDGER GREEN'),
    (re.compile(r'(?i)\b(?:LEGALZOOM|LZC|FORMATION)\b'), 'LEGALZOOM'),
    (re.compile(r'(?i)\b(?:LHI)\b'), 'LHI'),
    (re.compile(r'(?i)\b(?:0441\ OUTSIDE|0807\ OUTSIDE)\b'), 'LOVE\'S #0807 OUTSIDE'),
    (re.compile(r'(?i)\b(?:LOWES)\b'), 'LOWES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:MANDELA\ PARTNERS\ LLC|CCD\ \-\ SUBLIME\ MACHINING\ MANDELA\ PARTNERS\ LLC)(?![A-Z0-9])'), 'MANDELA PARTNERS LLC'),
    (re.compile(r'(?i)\b(?:MARISA\ K\ BADUA)\b'), 'MARISA K BADUA'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:MASH\ GAS\ \&AMP)(?![A-Z0-9])'), 'MASH GAS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:MATRIX\ TRUST\ CO\ PAYMENT|MATRIX\ TRUST\ CO\-PAYMENT)(?![A-Z0-9])'), 'MATRIX TRUST'),
    (re.compile(r'(?i)\b(?:MEDIAJEL|PAYMENT\ MEDIAJEL)\b'), 'MEDIAJEL'),
    (re.compile(r'(?i)\b(?:MEDIWASTE\ DISPOSAL|MEDIWASTE\ DISPOSAL\ LLC)\b'), 'MEDIWASTE DISPOSAL LLC'),
    (re.compile(r'(?i)\b(?:MESA\ OUTDOOR\ BILLBOAR|MESA\ OUTDOOR\ PARTNERS|MESA\ OUTDOOR\ PARTN)\b'), 'MESA'),
    (re.compile(r'(?i)\b(?:METRO\ PUBLISHING)\b'), 'METRO PUBLISHING'),
    (re.compile(r'(?i)\b(?:MG\ TRUST\ 0000000406|406)\b'), 'MG TRUST'),
    (re.compile(r'(?i)\b(?:MOMENTUM\ GPS)\b'), 'MOMENTUM GPS'),
    (re.compile(r'(?i)\b(?:MOMENTUM\ IOT)\b'), 'MOMENTUM IOT'),
    (re.compile(r'(?i)\b(?:MORGAN\ AT\ PROVOST\ SQJERSEY\ CITY\ NJ)\b'), 'MORGAN AT PROVOST'),
    (re.compile(r'(?i)\b(?:MORGAN\ AT\ PROVOST\ SQUARE)\b'), 'MORGAN AT PROVOST SQUARE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:MARK\ SCHNEIDERMAN\ INSURAN|EXTERNAL\ WITHDRAWAL\ MRKSCHNDRMNINSRN\ 855\-872\-6632\ \-\ AUTOPAY)(?![A-Z0-9])'), 'MRKSCHNDRMNINSRN'),
    (re.compile(r'(?i)\b(?:MSFT)\b'), 'MSFT'),
    (re.compile(r'(?i)\b(?:DBA\ FOUR\ C\ 3523\ MT\ DIABLO\ BLV\ US\ LAFAYETTE)\b'), 'MT DIABLO'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:GREENBAXMRB\-ACCOUNTFEE|GREENBAXDEBITCAR\-ACCOUNTFEE)(?![A-Z0-9])'), 'NBCU'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:NEXT\ WAVE\ PREMIU\ INS\.PMTS\.|INS\.PMTS\.)(?![A-Z0-9])'), 'NEXT WAVE'),
    (re.compile(r'(?i)\b(?:NYCDOT\ PARKING\ METERS)\b'), 'NYCDOT PARKING METERS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:DEPOSITBRIDGEPLUS\ DEPOSIT|CURRENCY\ AND\ COIN\ DEPOSITED\ 00000000000|754|CREDIT\ ADJUSTMENT\ 00000000000|615|DEPOSITED\ ITEM\ RETURNED|GBX\ ACH\-NORTH\ BAY\ CU|000\.00\ W/O\ 02)(?![A-Z0-9])'), 'ODOO B2B'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ODOO\ INC\.)(?![A-Z0-9])'), 'ODOO INC.'),
    (re.compile(r'(?i)\b(?:OIL\ CHANGER\ HQ)\b'), 'OIL CHANGER HQ'),
    (re.compile(r'(?i)\b(?:ONFLEET)\b'), 'ONFLEET'),
    (re.compile(r'(?i)\b(?:ONLINE\ LABELS)\b'), 'ONLINE LABELS, INC.'),
    (re.compile(r'(?i)\b(?:DESCRIPTIVE\ WITHDRAWAL\ ACH\ PREFUNDING\ BA|DOMESTIC\ WIRE\ WTH\ FEE\ OUTGOING\ WIRE\ FEE|WITHDRAWAL\ ACH\ PREFUNDING\ OSS\ CREDIT\ BUSINESSES|WWW)\b'), 'OSS'),
    (re.compile(r'(?i)\b(?:DOMESTIC\ WIRE\ DEPOSIT\ WIRE\ FROM\ OU\ SAEFONG)\b'), 'OU SAEFONG'),
    (re.compile(r'(?i)\b(?:PANOCHE\ FOOD\ MART)\b'), 'PANOCHE FOOD MART'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:60776\ \-\ SFO\ PARKINGCENTRA|60776\-SFO\ PREPAID\ PASAN\ FRANCISCO\ CA)(?![A-Z0-9])'), 'PARKINGCENTRAL'),
    (re.compile(r'(?i)\b(?:PATCHWORK\ FARMS)\b'), 'PATCHWORK FARMS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:1048|90803|90803\-1047|DELIVERMD|DELIVERMD\ BILL|DELIVERMD\ DBA\ SMOAKLAND|DELIVERMD\ DD|DELIVERMD\ GPS|DELIVERMD\ TAX|DELIVERMD\-BILL|DELIVERMD\-DD|DELIVERMD\-GPS|DELIVERMD\-TAX|GBX\ CHECK|PAYMENT\ BRIANA\ GONZALEZ|PAYMENT\ DELIVERMD\ DBA\ SMOAKLAND|PAYMENT\ ERIC\ GATPANDAN|PAYMENT\ ESMERALDA\ ROMERO|PAYMENT\ IRMA\ MENDOZA\-PEREZ|PAYMENT\ JAVEN\ JOHNSON|PAYMENT\ JOSE\ GOMES|PAYMENT\ JUNIOR\ DIAZ|PAYMENT\ KANDY\ PEREZ|PAYMENT\ KEVIN\ ALLEN|PAYMENT\ LEONIDAS\ MARTINEZ|PAYMENT\ MARIA\ RAMIREZ\ CISNEROS|PAYMENT\ MICHAEL\ BARR|PAYMENT\ NATALIE\ SORRELL|PAYMENT\ OSCAR\ QUIROZ\ ALVAREZ|PAYMENT\ TERRELL\ KELLY|PAYMENT\ WENDI\ CASADOS|PAYMENT\ YENI\ MORALES|PAYROLLSYS|PAYROLL\ SYS\ PTB\ PAYROLLSYS|SMO|SMOAKLAND\ OAKLAN\-PURCHASE|SMOAKLAND\ SUPPLY\ BILL|SMOAKLAND\ SUPPLY\ DD|SMOAKLAND\ SUPPLY\ TAX|SMOAKLAND\ WEED\ D\-BILL|SMOAKLAND\ WEED\ D\-DD|SMOAKLAND\ WEED\ D\-GPS|SMOAKLAND\ WEED\ D\-TAX|PAYNW|DELIVERMD[- ]DD:?90803|DELIVERMD[- ](BILL|GPS|TAX))(?![A-Z0-9])'), 'PAYNW'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:PG\&AMP|E/EZ\-PAY|WEB\ ONLINE|PGANDE\ WEB\ ONLINE)(?![A-Z0-9])'), 'PG&E'),
    (re.compile(r'(?i)\b(?:PILOT\ 168)\b'), 'PILOT'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:PLIVO\.COM)(?![A-Z0-9])'), 'PLIVO'),
    (re.compile(r'(?i)\b(?:PRIMO\ WATER)\b'), 'PRIMO WATER'),
    (re.compile(r'(?i)\b(?:FR\ PRIMPATCHARA\ ARORA)\b'), 'PRIMPATCHARA ARORA'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:PS\ ADMIN\ PSADMINIST|PSADMINIST|PS\ ADMIN\-PSADMINIST)(?![A-Z0-9])'), 'PS ADMINISTRATORS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:INTUIT|QBOOKS\ ONL|QBOOKS\ ONLINE|INTUIT\ \-QBOOKS\ ONL)(?![A-Z0-9])'), 'QUICKBOOKS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:READYREFRESH/WATERSERV)(?![A-Z0-9])'), 'READYREFRESH'),
    (re.compile(r'(?i)\b(?:ROOT\ SCIENCES)\b'), 'ROOT SCIENCES'),
    (re.compile(r'(?i)\b(?:ROTTEN\ ROBBIE)\b'), 'ROTTEN ROBBIE'),
    (re.compile(r'(?i)\b(?:SACRAMENTO\ MUNICIPAL)\b'), 'SACRAMENTO MUNICIPAL'),
    (re.compile(r'(?i)\b(?:WITHDRAWAL\ ACH\ PREFUNDING\ SACRAMENTO\ RENT\ CREDIT\ BUSINESSES)\b'), 'SACRAMENTO RENT'),
    (re.compile(r'(?i)\b(?:SAMSARA)\b'), 'SAMSARA'),
    (re.compile(r'(?i)\b(?:SECURITY\ MARKETING\ KING)\b'), 'SECURITY MARKETING KING'),
    (re.compile(r'(?i)\b(?:SEMRUSH)\b'), 'SEMRUSH'),
    (re.compile(r'(?i)\b(?:SENTRY)\b'), 'SENTRY'),
    (re.compile(r'(?i)\b(?:HDN|SHAY\ AARON\ GILMOR)\b'), 'SHAY AARON GILMOR'),
    (re.compile(r'(?i)\b(?:SHELL\ OIL\ 57444479901|SHELL\ OIL\ 10007822009|SHELL\ SERVICE\ STATIOOAKLAND\ CA|SHELL\ SERVICE\ STATION|SHELL\ SERVICE\ STATIOORINDA\ CA|SHELL\ OIL\ 57444479802|SHELL\ OIL\ 57445612401|SHELL\ OIL\ 57444216501|SHELL\ SERVICE\ STATIOCASTAIC\ CA|SHELL\ OIL\ 57444676308|SHELL\ OIL\ 57444480909|SHELL\ OIL\ 57444479901\ ORINDA\ CA)\b'), 'SHELL'),
    (re.compile(r'(?i)\b(?:SHOPIFY)\b'), 'SHOPIFY'),
    (re.compile(r'(?i)\b(?:SIBANNA\ CONSULTANT)\b'), 'SIBANNA CONSULTANTS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:SOUTHWES\s+[0-9]{6,}|SOUTHWEST)(?![A-Z0-9])'), 'SOUTHWEST'),
    (re.compile(r'(?i)\b(?:SPAGHETTI\ NETWOR\ SALE)\b'), 'SPAGHETTI NETWORK INC'),
    (re.compile(r'(?i)\b(?:SPARKS\ MARKETING\ G)\b'), 'SPARKS MARKETING'),
    (re.compile(r'(?i)\b(?:SPECTRUM)\b'), 'SPECTRUM'),
    (re.compile(r'(?i)\b(?:SPEEDWAY\ 01474\ 161\ FRAZIE|SPEEDWAY\ 46174)\b'), 'SPEEDWAY'),
    (re.compile(r'(?i)\b(?:PAYMENT\ SPENCER\ YOUNG\ LAW\ PC)\b'), 'SPENCER YOUNG LAW PC'),
    (re.compile(r'(?i)\b(?:SPIRIT\ AIRLINES\ ONBOARD\ R)\b'), 'SPIRIT AIRLINES'),
    (re.compile(r'(?i)\b(?:SPRINGBIG)\b'), 'SPRINGBIG'),
    (re.compile(r'(?i)\b(?:SP\ PRM\ FILTRATION)\b'), 'SP PRM FILTRATION'),
    (re.compile(r'(?i)\b(?:STAR\ COPY)\b'), 'STAR COPY'),
    (re.compile(r'(?i)\b(?:STEADY\ DEMAND)\b'), 'STEADY DEMAND'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:INS\.\ PMNT\.|STONEMARK\ INC\ INS\.\ PMNT\.|PFS\ FINANCING\ CO\ INS\.\ PMNT\.)(?![A-Z0-9])'), 'STONEMARK'),
    (re.compile(r'(?i)\b(?:STOP\ PAY\ FEE)\b'), 'STOP PAY FEE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:STRONGHOLD\-SETTLEMENT\ TOTAL\ DEPOSIT|STRONGHOLD|SETTLEMENT\ STRONGHOLD\ SMOAKLANDPORTH|SETTLEMENT|ACH\ CREDIT)(?![A-Z0-9])'), 'STRONGHOLD'),
    (re.compile(r'(?i)\b(?:SUBLIME\ MACHINING)\b'), 'SUBLIME MACHINING'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:SUBLIME\ MACHINING\ INC|INC\.\ CHECKING\ \-\ 28\-03\-01\-001875|ACCOUNT_TO_ACCOUNT\ SUBLIME\ MACHINING)(?![A-Z0-9])'), 'SUBLIME MACHINING INC'),
    (re.compile(r'(?i)\b(?:EXTERNAL\ DEPOSIT\ SWITCH\ COMMERCE\ 1700000|SWITCH\ COMMERCE|DEPOSIT\ SWITCH\ COMMERCE)\b'), 'SWITCH COMMERCE'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:TARGET\.COM)(?![A-Z0-9])'), 'TARGET'),
    (re.compile(r'(?i)\b(?:THE\ HOME\ DEPOT|0627)\b'), 'THE HOME DEPOT'),
    (re.compile(r'(?i)\b(?:THE\ MORGAN\ AT\ PROVOST\ SQ)\b'), 'THE MORGAN'),
    (re.compile(r'(?i)\b(?:THE\ PUREST\ HEART\ 786\ INC)\b'), 'THE PUREST HEART 786 INC'),
    (re.compile(r'(?i)\b(?:THE\ WEBSTAURANT\ STORE\ INC)\b'), 'THE WEBSTAURANT STORE INC'),
    (re.compile(r'(?i)\b(?:RETAIL_PAY|TOYOTA\ FINANCIAL\ RETAIL_PAY)\b'), 'TOYOTA FINANCIAL'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:TRELLO\.COM|ATLASSIAN)(?![A-Z0-9])'), 'TRELLO'),
    (re.compile(r'(?i)\b(?:TRINITY\ CONSULTING\ AND\ HOLDINGS)\b'), 'TRINITY CONSULTING AND HOLDINGS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EXTERNAL\ DEPOSIT\ TUV\ INVESTMENTS\ \-\ TUV\ INVEST|TUV\ INVESTMENTS\ TUV\ INVEST|TUV\ INVEST|TUV\ INVEST\ TUV\ INVESTMENTS\ TUV\ INVEST)(?![A-Z0-9])'), 'TUV INVESTMENTS'),
    (re.compile(r'(?i)\b(?:BILL_PAY\ TWENTY8GRAMZ\ TWENTY8GRAMZ)\b'), 'TWENTY8GRAMZ'),
    (re.compile(r'(?i)\b(?:TWILIO\ SENDGRID)\b'), 'TWILIO'),
    (re.compile(r'(?i)\b(?:TWITCHTVDON)\b'), 'TWITCHTVDON'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:TMOBILE|T\-MOBILE\ PCS\ SVC|PCS\ SVC|PREPD\ AUTOPY|AUTO\ PAY)(?![A-Z0-9])'), 'T-MOBILE'),
    (re.compile(r'(?i)\b(?:ULINE|SHIP\ SUPPLIES)\b'), 'ULINE'),
    (re.compile(r'(?i)\b(?:UNION\ 76\ 09530684\ OAKLAND\ CA)\b'), 'UNION'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:UPGRADED\ BOARDING\ CREDIT|UNITED\s+0?16[0-9]{6,})(?![A-Z0-9])'), 'UNITED'),
    (re.compile(r'(?i)\b(?:UPRINTING)\b'), 'UPRINTING'),
    (re.compile(r'(?i)\b(?:PAYMENTUS\ CORPORATION)\b'), 'US CORPORATION'),
    (re.compile(r'(?i)\b(?:PAYPAL\ ACCTVERIFY)\b'), 'VENMO'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:VERCEL\ INC\.|VERCEL\ PRO)(?![A-Z0-9])'), 'VERCEL'),
    (re.compile(r'(?i)\b(?:VISTAPRINT)\b'), 'VISTAPRINT'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:WASTE\ MGMT\ WM\ EZPAY|WM\.COM|WM\.COM\ 866\-909\-4458\ TX|UTILITIES)(?![A-Z0-9])'), 'WASTE MGMT'),
    (re.compile(r'(?i)\b(?:WEINSTEIN\ LOCAL)\b'), 'WEINSTEIN LOCAL'),
    (re.compile(r'(?i)\b(?:WEST\ TOWN\ BNK\ DAILYPAYME|DAILYPAYME)\b'), 'WEST TOWN BNK'),
    (re.compile(r'(?i)\b(?:WILTON)\b'), 'WILTON'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:EXTERNAL\ WITHDRAWAL\ WORLDWIDE\ ATM\ LL\ \-\ SALE|EXTERNAL\ WITHDRAWAL\ WORLDWIDE\ ATM\ LL\ \-)(?![A-Z0-9])'), 'WORLDWIDE ATM'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:WYKOWSKI\ \&\ WOOD(?:\ LLP)?)(?![A-Z0-9])'), 'WYKOWSKI & WOOD LLP'),
    (re.compile(r'(?i)\b(?:WITHDRAWAL\ ACH\ PREFUNDING\ XEVEN\ SOLUTIONS\ CREDIT\ BUSINESSES)\b'), 'XEVEN SOLUTIONS'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:YELLOWIMAGES\.COM)(?![A-Z0-9])'), 'YELLOWIMAGES'),
    (re.compile(r'(?i)(?<![A-Z0-9])(?:ZOOM\.US\ 888\-799\-9666)(?![A-Z0-9])'), 'ZOOM'),
]

# --- Helpers -----------------------------------------------------------------

def _normalize_text(s: str) -> str:
    """Normalize string casing and spacing for consistent regex matching."""
    if not isinstance(s, str):
        return ""
    s = s.upper().strip()
    return " ".join(s.split())

def _concat_row(row: dict) -> str:
    """
    Concatenate the key source columns into one normalized text string,
    tagging each segment with the column name — same pattern as entity_qbo.
    """
    parts = []
    for c in _SOURCE_COLS:
        v = row.get(c, "") or ""
        v = str(v)
        if v.strip():
            parts.append(f"{c.upper()}: {v}")

    return _normalize_text(" | ".join(parts))

def build_search_text(df: pd.DataFrame) -> pd.Series:
    """
    Vectorized: produce same text as _concat_row() for each row.
    Ensures rule matching is consistent between infer() and apply_rules().
    """
    return df.apply(lambda r: _concat_row(r.to_dict()), axis=1)

def postprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply non-text overrides after regex rules.
    Rule: if description contains CHECK (case-insensitive, word boundary) and amount < 0,
    force payee_vendor to PAYNW and mark lineage as postprocess.
    """
    if df is None or df.empty:
        return df
    if not {"description", "amount", "payee_vendor"}.issubset(df.columns):
        return df

    out = df.copy()
    desc = out["description"].fillna("").astype(str)
    amounts = pd.to_numeric(out["amount"], errors="coerce")
    mask = desc.str.contains(_CHECK_PATTERN) & (amounts < 0)
    if not mask.any():
        return out

    out.loc[mask, "payee_vendor"] = "PAYNW"

    for tag_col in ("payee_vendor_rule_tag", "payee_rule_tag"):
        if tag_col in out.columns:
            out.loc[mask, tag_col] = _POSTPROCESS_TAG

    for src_col in ("payee_vendor_source", "payee_source"):
        if src_col in out.columns:
            out.loc[mask, src_col] = _POSTPROCESS_SOURCE

    for conf_col in ("payee_vendor_confidence", "payee_confidence"):
        if conf_col in out.columns:
            out.loc[mask, conf_col] = 1.0

    if "payee_rule_id" in out.columns:
        out.loc[mask, "payee_rule_id"] = pd.NA

    return out

def infer(row: dict) -> tuple[str, str]:
    """
    Row-level API.
    Returns:
        (payee_vendor_value, rule_tag)
    """
    text = _concat_row(row)

    for idx, (pat, payee) in enumerate(_RULES, start=1):
        matched = pat.search(text) if hasattr(pat, "search") else re.search(pat, text)
        if matched:
            return payee, f"{_RULEBOOK_NAME}@{_RULEBOOK_VERSION}#{idx}"

    return "UNKNOWN", ""

def apply_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized rule application.
    Adds:
        - payee_vendor
        - payee_rule_id
        - payee_rule_tag
        - payee_confidence
        - payee_source
    """
    out = df.copy()
    search = build_search_text(out)

    out["payee_vendor"] = ""
    out["payee_rule_id"] = pd.Series([pd.NA] * len(out), dtype="Int64")
    out["payee_rule_tag"] = ""
    out["payee_confidence"] = 0.0
    out["payee_source"] = "unknown"

    unmatched = out["payee_vendor"] == ""

    for i, (pat, payee) in enumerate(_RULES, start=1):
        mask = unmatched & search.str.contains(pat, regex=True)
        if not mask.any():
            continue

        out.loc[mask, "payee_vendor"] = payee
        out.loc[mask, "payee_rule_id"] = i
        out.loc[mask, "payee_rule_tag"] = f"{_RULEBOOK_NAME}@{_RULEBOOK_VERSION}#{i}"
        out.loc[mask, "payee_confidence"] = 1.0
        out.loc[mask, "payee_source"] = "rule"

        unmatched = out["payee_vendor"] == ""

    out.loc[out["payee_vendor"] == "", "payee_vendor"] = "UNKNOWN"
    return postprocess(out)
