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
    (re.compile(r'(?i)(?<![A-Za-z0-9])PAYROLL(?![A-Za-z0-9])'), ' HR '),  # support=5556 purity=0.9995
    (re.compile(r'(?i)(?<![A-Za-z0-9])LABOR(?![A-Za-z0-9])'), ' HR '),  # support=5550 purity=0.9996
    (re.compile(r'(?i)(?<![A-Za-z0-9])REVENUE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=4597 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PAYNW(?![A-Za-z0-9])'), ' HR '),  # support=3452 purity=0.9994
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEPOSIT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=3030 purity=0.9784
    (re.compile(r'(?i)(?<![A-Za-z0-9])CANNABIS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=2238 purity=0.9987
    (re.compile(r'(?i)(?<![A-Za-z0-9])DIRECT(?![A-Za-z0-9])'), ' HR '),  # support=1954 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GUSTO(?![A-Za-z0-9])'), ' HR '),  # support=1830 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEREK(?![A-Za-z0-9])'), ' Sublime '),  # support=1614 purity=0.9908
    (re.compile(r'(?i)(?<![A-Za-z0-9])SWITCH(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=1206 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])COMMERCE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=1206 purity=0.9992
    (re.compile(r'(?i)(?<![A-Za-z0-9])DOUG(?![A-Za-z0-9])'), ' Technology '),  # support=1113 purity=0.9521
    (re.compile(r'(?i)(?<![A-Za-z0-9])APPS(?![A-Za-z0-9])'), ' Technology '),  # support=1076 purity=0.9197
    (re.compile(r'(?i)(?<![A-Za-z0-9])DENNIS(?![A-Za-z0-9])'), ' Marketing '),  # support=1062 purity=0.9852
    (re.compile(r'(?i)(?<![A-Za-z0-9])MARKETING(?![A-Za-z0-9])'), ' Marketing '),  # support=1061 purity=0.9681
    (re.compile(r'(?i)(?<![A-Za-z0-9])CHANNA(?![A-Za-z0-9])'), ' Finance '),  # support=1050 purity=0.9598
    (re.compile(r'(?i)(?<![A-Za-z0-9])PROMOTION(?![A-Za-z0-9])'), ' Marketing '),  # support=1048 purity=0.974
    (re.compile(r'(?i)(?<![A-Za-z0-9])ADVERTISING(?![A-Za-z0-9])'), ' Marketing '),  # support=1048 purity=0.974
    (re.compile(r'(?i)(?<![A-Za-z0-9])CASH(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=761 purity=0.9719
    (re.compile(r'(?i)(?<![A-Za-z0-9])X0435(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=635 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])X0586(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=635 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])B2B(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=606 purity=0.9934
    (re.compile(r'(?i)(?<![A-Za-z0-9])ODOO(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=569 purity=0.9844
    (re.compile(r'(?i)(?<![A-Za-z0-9])JAY(?![A-Za-z0-9])'), ' Executive '),  # support=567 purity=0.9982
    (re.compile(r'(?i)(?<![A-Za-z0-9])STRONGHOLD(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=541 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INVESTMENTS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=480 purity=0.9979
    (re.compile(r'(?i)(?<![A-Za-z0-9])INVEST(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=470 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TUV(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=470 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SCHG(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=456 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CAPITAL(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=456 purity=0.96
    (re.compile(r'(?i)(?<![A-Za-z0-9])ONE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=450 purity=0.9636
    (re.compile(r'(?i)(?<![A-Za-z0-9])FACILITY(?![A-Za-z0-9])'), ' Administration '),  # support=434 purity=0.9864
    (re.compile(r'(?i)(?<![A-Za-z0-9])SETTLEMENT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=418 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])1700000586(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=414 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])1700000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=414 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])160435(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=414 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEPOSITBRIDGEPLUS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=391 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])RECEIVED(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=391 purity=0.9824
    (re.compile(r'(?i)(?<![A-Za-z0-9])THANK(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=381 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])YOU(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=381 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])90803(?![A-Za-z0-9])'), ' HR '),  # support=349 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])OVERHEAD(?![A-Za-z0-9])'), ' Sublime '),  # support=319 purity=0.93
    (re.compile(r'(?i)(?<![A-Za-z0-9])CHASE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=304 purity=0.9441
    (re.compile(r'(?i)(?<![A-Za-z0-9])TOTAL(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=291 purity=0.9765
    (re.compile(r'(?i)(?<![A-Za-z0-9])CRD(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=281 purity=0.9398
    (re.compile(r'(?i)(?<![A-Za-z0-9])EPAY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=280 purity=0.9211
    (re.compile(r'(?i)(?<![A-Za-z0-9])SERIAL(?![A-Za-z0-9])'), ' HR '),  # support=279 purity=0.9929
    (re.compile(r'(?i)(?<![A-Za-z0-9])APSMOAKLAND(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=273 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FASTRAK(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=266 purity=0.911
    (re.compile(r'(?i)(?<![A-Za-z0-9])SMOAKLANDSAC(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=261 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PAYM(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=251 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DAILY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=251 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INTERCOMPANY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=235 purity=0.9958
    (re.compile(r'(?i)(?<![A-Za-z0-9])TELEPHONE(?![A-Za-z0-9])'), ' Administration '),  # support=233 purity=0.9957
    (re.compile(r'(?i)(?<![A-Za-z0-9])NORTH(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=226 purity=0.9536
    (re.compile(r'(?i)(?<![A-Za-z0-9])PAYMENTCREDIT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=223 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ADOBE(?![A-Za-z0-9])'), ' Technology '),  # support=222 purity=0.9823
    (re.compile(r'(?i)(?<![A-Za-z0-9])RAJ(?![A-Za-z0-9])'), ' Driver Operations - Sacramento '),  # support=218 purity=0.9277
    (re.compile(r'(?i)(?<![A-Za-z0-9])GREEN(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=206 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LEDGER(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=199 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STRONGHOLDSETTLEMENT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=173 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])100(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=155 purity=0.9451
    (re.compile(r'(?i)(?<![A-Za-z0-9])BANKARMOR(?![A-Za-z0-9])'), ' Finance '),  # support=151 purity=0.9497
    (re.compile(r'(?i)(?<![A-Za-z0-9])QUICKBOOKS(?![A-Za-z0-9])'), ' Technology '),  # support=140 purity=0.9333
    (re.compile(r'(?i)(?<![A-Za-z0-9])INTUIT(?![A-Za-z0-9])'), ' Technology '),  # support=138 purity=0.9324
    (re.compile(r'(?i)(?<![A-Za-z0-9])GPS(?![A-Za-z0-9])'), ' HR '),  # support=136 purity=0.9645
    (re.compile(r'(?i)(?<![A-Za-z0-9])QBOOKS(?![A-Za-z0-9])'), ' Technology '),  # support=136 purity=0.9315
    (re.compile(r'(?i)(?<![A-Za-z0-9])CASHLESS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=132 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MATRIX(?![A-Za-z0-9])'), ' HR '),  # support=130 purity=0.9924
    (re.compile(r'(?i)(?<![A-Za-z0-9])UNIFORMS(?![A-Za-z0-9])'), ' Sublime '),  # support=118 purity=0.9833
    (re.compile(r'(?i)(?<![A-Za-z0-9])WEST(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=116 purity=0.9748
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEPOSITED(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=116 purity=0.9748
    (re.compile(r'(?i)(?<![A-Za-z0-9])CINTAS(?![A-Za-z0-9])'), ' Sublime '),  # support=116 purity=0.9748
    (re.compile(r'(?i)(?<![A-Za-z0-9])DAILYPAYME(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=110 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TOWN(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=110 purity=0.991
    (re.compile(r'(?i)(?<![A-Za-z0-9])BNK(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=110 purity=0.991
    (re.compile(r'(?i)(?<![A-Za-z0-9])KLAVIYO(?![A-Za-z0-9])'), ' Marketing '),  # support=105 purity=0.9906
    (re.compile(r'(?i)(?<![A-Za-z0-9])COIN(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=103 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CURRENCY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=103 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])OUTDOOR(?![A-Za-z0-9])'), ' Marketing '),  # support=99 purity=0.99
    (re.compile(r'(?i)(?<![A-Za-z0-9])200(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=99 purity=0.9252
    (re.compile(r'(?i)(?<![A-Za-z0-9])COMCAST(?![A-Za-z0-9])'), ' Administration '),  # support=95 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DOORDASH(?![A-Za-z0-9])'), ' Administration '),  # support=95 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DINING(?![A-Za-z0-9])'), ' Administration '),  # support=92 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BLAZE(?![A-Za-z0-9])'), ' Technology '),  # support=87 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PMTS(?![A-Za-z0-9])'), ' Administration '),  # support=81 purity=0.9878
    (re.compile(r'(?i)(?<![A-Za-z0-9])GHOST(?![A-Za-z0-9])'), ' Marketing '),  # support=79 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])OAK(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=78 purity=0.975
    (re.compile(r'(?i)(?<![A-Za-z0-9])EASTWEST(?![A-Za-z0-9])'), ' Finance '),  # support=74 purity=0.9867
    (re.compile(r'(?i)(?<![A-Za-z0-9])PLIVO(?![A-Za-z0-9])'), ' Marketing '),  # support=68 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PLIVOCOM(?![A-Za-z0-9])'), ' Marketing '),  # support=68 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACTIVITY(?![A-Za-z0-9])'), ' Finance '),  # support=66 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ANALYSIS(?![A-Za-z0-9])'), ' Finance '),  # support=66 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MISC(?![A-Za-z0-9])'), ' Finance '),  # support=66 purity=0.9706
    (re.compile(r'(?i)(?<![A-Za-z0-9])TMOBILE(?![A-Za-z0-9])'), ' Administration '),  # support=65 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HARRENS(?![A-Za-z0-9])'), ' Sublime '),  # support=65 purity=0.9848
    (re.compile(r'(?i)(?<![A-Za-z0-9])HUMAN(?![A-Za-z0-9])'), ' HR '),  # support=63 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ONL(?![A-Za-z0-9])'), ' Technology '),  # support=63 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])AEROPAYAEROPAY(?![A-Za-z0-9])'), ' Finance '),  # support=63 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INTEREST(?![A-Za-z0-9])'), ' HR '),  # support=63 purity=0.9844
    (re.compile(r'(?i)(?<![A-Za-z0-9])2999(?![A-Za-z0-9])'), ' Technology '),  # support=61 purity=0.9839
    (re.compile(r'(?i)(?<![A-Za-z0-9])SUBLIM(?![A-Za-z0-9])'), ' Sublime '),  # support=58 purity=0.9831
    (re.compile(r'(?i)(?<![A-Za-z0-9])PETROL(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=56 purity=0.9825
    (re.compile(r'(?i)(?<![A-Za-z0-9])OTC(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=55 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INDIVIDUAL(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=54 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACC(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=54 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])AUTOMATIC(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=54 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EMPYREAL(?![A-Za-z0-9])'), ' Finance '),  # support=54 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CLOUD(?![A-Za-z0-9])'), ' Technology '),  # support=54 purity=0.9818
    (re.compile(r'(?i)(?<![A-Za-z0-9])CARD(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=53 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])41600(?![A-Za-z0-9])'), ' Administration '),  # support=51 purity=0.9808
    (re.compile(r'(?i)(?<![A-Za-z0-9])SPIRIT(?![A-Za-z0-9])'), ' Executive '),  # support=51 purity=0.9444
    (re.compile(r'(?i)(?<![A-Za-z0-9])AIRLINES(?![A-Za-z0-9])'), ' Executive '),  # support=51 purity=0.9273
    (re.compile(r'(?i)(?<![A-Za-z0-9])EMPYREALENTERPRI(?![A-Za-z0-9])'), ' Finance '),  # support=50 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BILLB(?![A-Za-z0-9])'), ' Marketing '),  # support=49 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INTE(?![A-Za-z0-9])'), ' HR '),  # support=49 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])00000000000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=49 purity=0.9074
    (re.compile(r'(?i)(?<![A-Za-z0-9])A35810(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=47 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DNS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=47 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])1700(?![A-Za-z0-9])'), ' Executive '),  # support=45 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])AIRL(?![A-Za-z0-9])'), ' Executive '),  # support=44 purity=0.9362
    (re.compile(r'(?i)(?<![A-Za-z0-9])20538(?![A-Za-z0-9])'), ' HR '),  # support=43 purity=0.9773
    (re.compile(r'(?i)(?<![A-Za-z0-9])TWILIO(?![A-Za-z0-9])'), ' Marketing '),  # support=43 purity=0.9773
    (re.compile(r'(?i)(?<![A-Za-z0-9])JOINTCOMMERCE(?![A-Za-z0-9])'), ' Marketing '),  # support=42 purity=0.9767
    (re.compile(r'(?i)(?<![A-Za-z0-9])GAIN(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=41 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FCU(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=41 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])754(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=39 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])121182860000005(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=39 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])RDC(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=38 purity=0.95
    (re.compile(r'(?i)(?<![A-Za-z0-9])LHI(?![A-Za-z0-9])'), ' Marketing '),  # support=37 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ELECTRICITY(?![A-Za-z0-9])'), ' Sublime '),  # support=37 purity=0.9487
    (re.compile(r'(?i)(?<![A-Za-z0-9])ADS(?![A-Za-z0-9])'), ' Marketing '),  # support=36 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STOCK(?![A-Za-z0-9])'), ' Technology '),  # support=36 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])AMEX(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=36 purity=0.973
    (re.compile(r'(?i)(?<![A-Za-z0-9])WEINSTEIN(?![A-Za-z0-9])'), ' Administration '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MANDELA(?![A-Za-z0-9])'), ' Sublime '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EPAYMENT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ALIBABA(?![A-Za-z0-9])'), ' Sublime '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PARTN(?![A-Za-z0-9])'), ' Marketing '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HONEY(?![A-Za-z0-9])'), ' Administration '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])15303(?![A-Za-z0-9])'), ' Administration '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BUCKET(?![A-Za-z0-9])'), ' Administration '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])VERCEL(?![A-Za-z0-9])'), ' Technology '),  # support=35 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])5499(?![A-Za-z0-9])'), ' Technology '),  # support=35 purity=0.9722
    (re.compile(r'(?i)(?<![A-Za-z0-9])050(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=35 purity=0.9722
    (re.compile(r'(?i)(?<![A-Za-z0-9])LOCAL(?![A-Za-z0-9])'), ' Administration '),  # support=35 purity=0.9459
    (re.compile(r'(?i)(?<![A-Za-z0-9])CABBAGE(?![A-Za-z0-9])'), ' Marketing '),  # support=34 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EEZPAY(?![A-Za-z0-9])'), ' Sublime '),  # support=34 purity=0.9444
    (re.compile(r'(?i)(?<![A-Za-z0-9])PGAMP(?![A-Za-z0-9])'), ' Sublime '),  # support=34 purity=0.9444
    (re.compile(r'(?i)(?<![A-Za-z0-9])GUSTOPAYMENT(?![A-Za-z0-9])'), ' HR '),  # support=33 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])AMZNCOMBILL(?![A-Za-z0-9])'), ' Administration '),  # support=33 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ECHTRAI(?![A-Za-z0-9])'), ' Administration '),  # support=33 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])XXXXXXXXXXXX8205(?![A-Za-z0-9])'), ' Administration '),  # support=33 purity=0.9706
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEPOS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=33 purity=0.9706
    (re.compile(r'(?i)(?<![A-Za-z0-9])BUSINESS(?![A-Za-z0-9])'), ' Administration '),  # support=33 purity=0.9167
    (re.compile(r'(?i)(?<![A-Za-z0-9])LLP(?![A-Za-z0-9])'), ' Administration '),  # support=32 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HUMANA(?![A-Za-z0-9])'), ' HR '),  # support=32 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])AIRCALL(?![A-Za-z0-9])'), ' Technology '),  # support=32 purity=0.9697
    (re.compile(r'(?i)(?<![A-Za-z0-9])GITHUB(?![A-Za-z0-9])'), ' Technology '),  # support=32 purity=0.9697
    (re.compile(r'(?i)(?<![A-Za-z0-9])ATT(?![A-Za-z0-9])'), ' Administration '),  # support=31 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PHONE(?![A-Za-z0-9])'), ' Administration '),  # support=31 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])280301000073(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=31 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ONFLEET(?![A-Za-z0-9])'), ' Driver Operations - Bay '),  # support=31 purity=0.9688
    (re.compile(r'(?i)(?<![A-Za-z0-9])AIRCALLIO(?![A-Za-z0-9])'), ' Technology '),  # support=31 purity=0.9688
    (re.compile(r'(?i)(?<![A-Za-z0-9])1499(?![A-Za-z0-9])'), ' Technology '),  # support=31 purity=0.9688
    (re.compile(r'(?i)(?<![A-Za-z0-9])MSFT(?![A-Za-z0-9])'), ' Technology '),  # support=30 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GSUITE(?![A-Za-z0-9])'), ' Technology '),  # support=30 purity=0.9375
    (re.compile(r'(?i)(?<![A-Za-z0-9])CITI(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=29 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])31676(?![A-Za-z0-9])'), ' Administration '),  # support=29 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INBOUND(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=29 purity=0.9355
    (re.compile(r'(?i)(?<![A-Za-z0-9])HISIG(?![A-Za-z0-9])'), ' HR '),  # support=29 purity=0.9062
    (re.compile(r'(?i)(?<![A-Za-z0-9])MUNICIPAL(?![A-Za-z0-9])'), ' Administration '),  # support=28 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CANVA(?![A-Za-z0-9])'), ' Technology '),  # support=28 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ANALYTIC(?![A-Za-z0-9])'), ' Marketing '),  # support=28 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NGO(?![A-Za-z0-9])'), ' Technology '),  # support=27 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])KONCAUSEWAY(?![A-Za-z0-9])'), ' Sublime '),  # support=27 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ASTRA(?![A-Za-z0-9])'), ' Administration '),  # support=27 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MOM(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=27 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])08420003447(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=27 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HONG(?![A-Za-z0-9])'), ' Sublime '),  # support=27 purity=0.9643
    (re.compile(r'(?i)(?<![A-Za-z0-9])ATLAS(?![A-Za-z0-9])'), ' HR '),  # support=27 purity=0.9643
    (re.compile(r'(?i)(?<![A-Za-z0-9])MEDIWASTE(?![A-Za-z0-9])'), ' Sublime '),  # support=27 purity=0.931
    (re.compile(r'(?i)(?<![A-Za-z0-9])0205456(?![A-Za-z0-9])'), 'Inventory - Wholesale'),  # support=27 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])786(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])08420003439(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PGANDE(?![A-Za-z0-9])'), ' Administration '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PCS(?![A-Za-z0-9])'), ' Administration '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])45575(?![A-Za-z0-9])'), ' HR '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DNH(?![A-Za-z0-9])'), ' Technology '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])APPFOLIO(?![A-Za-z0-9])'), ' Administration '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FED(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])23278(?![A-Za-z0-9])'), ' Sublime '),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ORIN(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=26 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])249(?![A-Za-z0-9])'), ' Administration '),  # support=26 purity=0.963
    (re.compile(r'(?i)(?<![A-Za-z0-9])7999(?![A-Za-z0-9])'), ' Technology '),  # support=26 purity=0.963
    (re.compile(r'(?i)(?<![A-Za-z0-9])ADT(?![A-Za-z0-9])'), ' Sublime '),  # support=25 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])5331(?![A-Za-z0-9])'), ' Administration '),  # support=25 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CALLRAIL(?![A-Za-z0-9])'), ' Technology '),  # support=25 purity=0.9615
    (re.compile(r'(?i)(?<![A-Za-z0-9])FORMSWIFT(?![A-Za-z0-9])'), ' Technology '),  # support=25 purity=0.9615
    (re.compile(r'(?i)(?<![A-Za-z0-9])FORMSWIFTCOMCHARGE(?![A-Za-z0-9])'), ' Technology '),  # support=25 purity=0.9615
    (re.compile(r'(?i)(?<![A-Za-z0-9])AUTOPY(?![A-Za-z0-9])'), ' Administration '),  # support=24 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PREPD(?![A-Za-z0-9])'), ' Administration '),  # support=24 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HARBOR(?![A-Za-z0-9])'), ' Administration '),  # support=24 purity=0.96
    (re.compile(r'(?i)(?<![A-Za-z0-9])VALLEY(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=24 purity=0.96
    (re.compile(r'(?i)(?<![A-Za-z0-9])5999(?![A-Za-z0-9])'), ' Technology '),  # support=24 purity=0.96
    (re.compile(r'(?i)(?<![A-Za-z0-9])CRDEPAY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])321177573(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SAMSARA(?![A-Za-z0-9])'), ' Technology '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])090116278(?![A-Za-z0-9])'), ' Sublime '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SPECTRUM(?![A-Za-z0-9])'), ' Technology '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])4798(?![A-Za-z0-9])'), ' Technology '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CREATIVE(?![A-Za-z0-9])'), ' Technology '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FLOR(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GRUPO(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=23 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MOMENTUM(?![A-Za-z0-9])'), ' Driver Operations - Bay '),  # support=23 purity=0.9583
    (re.compile(r'(?i)(?<![A-Za-z0-9])FEI(?![A-Za-z0-9])'), ' Administration '),  # support=22 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MARCO(?![A-Za-z0-9])'), ' Administration '),  # support=22 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CORPORATE(?![A-Za-z0-9])'), ' HR '),  # support=22 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ADS7475396058(?![A-Za-z0-9])'), ' Marketing '),  # support=22 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LABELS(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=22 purity=0.9565
    (re.compile(r'(?i)(?<![A-Za-z0-9])0378784CHEVLEBEC(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=21 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CLOVER(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=21 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])COMETCHAT(?![A-Za-z0-9])'), ' Technology '),  # support=21 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SUBS(?![A-Za-z0-9])'), ' Technology '),  # support=21 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACROPRO(?![A-Za-z0-9])'), ' Technology '),  # support=21 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PASSNY(?![A-Za-z0-9])'), ' New York '),  # support=21 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PASS(?![A-Za-z0-9])'), ' New York '),  # support=21 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MKTPLACE(?![A-Za-z0-9])'), ' Administration '),  # support=21 purity=0.9545
    (re.compile(r'(?i)(?<![A-Za-z0-9])WORLDWIDE(?![A-Za-z0-9])'), ' Finance '),  # support=21 purity=0.9545
    (re.compile(r'(?i)(?<![A-Za-z0-9])MCGRIFF(?![A-Za-z0-9])'), ' HR '),  # support=21 purity=0.913
    (re.compile(r'(?i)(?<![A-Za-z0-9])ALKHEMIST(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=20 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LC2400(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=20 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PMNT(?![A-Za-z0-9])'), ' Administration '),  # support=20 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STONEMARK(?![A-Za-z0-9])'), ' Administration '),  # support=20 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])08142023(?![A-Za-z0-9])'), ' HR '),  # support=20 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SMOAKLANDPORTH(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=20 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])REBILL(?![A-Za-z0-9])'), ' New York '),  # support=20 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TRELLO(?![A-Za-z0-9])'), ' Technology '),  # support=20 purity=0.9524
    (re.compile(r'(?i)(?<![A-Za-z0-9])TRELLOCOM(?![A-Za-z0-9])'), ' Technology '),  # support=20 purity=0.9524
    (re.compile(r'(?i)(?<![A-Za-z0-9])ATLASSIAN(?![A-Za-z0-9])'), ' Technology '),  # support=20 purity=0.9524
    (re.compile(r'(?i)(?<![A-Za-z0-9])PROPERTY(?![A-Za-z0-9])'), ' Administration '),  # support=20 purity=0.9524
    (re.compile(r'(?i)(?<![A-Za-z0-9])0378784(?![A-Za-z0-9])'), 'Inventory - Wholesale'),  # support=20 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])6250(?![A-Za-z0-9])'), ' Technology '),  # support=20 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])MOBILE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=20 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])ALPINE(?![A-Za-z0-9])'), ' Marketing '),  # support=19 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WHSE(?![A-Za-z0-9])'), ' Administration '),  # support=19 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])IOT(?![A-Za-z0-9])'), ' Driver Operations - Bay '),  # support=19 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])XPRESSCPTLATLAS(?![A-Za-z0-9])'), ' HR '),  # support=19 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACHTRANS(?![A-Za-z0-9])'), ' HR '),  # support=19 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])07312023(?![A-Za-z0-9])'), ' HR '),  # support=19 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FLORALS(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=19 purity=0.95
    (re.compile(r'(?i)(?<![A-Za-z0-9])LEGALZOOM(?![A-Za-z0-9])'), ' Administration '),  # support=18 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SQJERSEY(?![A-Za-z0-9])'), ' Executive '),  # support=18 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GODADDYCOM(?![A-Za-z0-9])'), ' Technology '),  # support=18 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PRIMO(?![A-Za-z0-9])'), ' Sublime '),  # support=18 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WTH(?![A-Za-z0-9])'), ' Finance '),  # support=18 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])08012023(?![A-Za-z0-9])'), ' HR '),  # support=18 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])KUSHMEN(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=18 purity=0.9474
    (re.compile(r'(?i)(?<![A-Za-z0-9])STATION(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=18 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])ZENITH(?![A-Za-z0-9])'), ' Marketing '),  # support=17 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STATIOOAKLAND(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=17 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BILLBOAR(?![A-Za-z0-9])'), ' Marketing '),  # support=17 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SPARKS(?![A-Za-z0-9])'), ' Marketing '),  # support=17 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])17708(?![A-Za-z0-9])'), ' Sublime '),  # support=17 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DISPENSARY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=17 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ROOTS(?![A-Za-z0-9])'), ' Marketing '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PEWESTLEY(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SENTRY(?![A-Za-z0-9])'), ' Administration '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GILBE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BRYANT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])57500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SACHDLGOV(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SACRAMEN(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ECOGREENINDUSTRIES(?![A-Za-z0-9])'), ' Sublime '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ECOGREENINDUSTRIESCOM(?![A-Za-z0-9])'), ' Sublime '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STEPHANIE(?![A-Za-z0-9])'), ' HR '),  # support=16 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEEP(?![A-Za-z0-9])'), ' Marketing '),  # support=16 purity=0.9412
    (re.compile(r'(?i)(?<![A-Za-z0-9])SCIENCES(?![A-Za-z0-9])'), ' Sublime '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ROOT(?![A-Za-z0-9])'), ' Sublime '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BILLBOARD(?![A-Za-z0-9])'), ' Marketing '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EDISON(?![A-Za-z0-9])'), ' Administration '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])57375(?![A-Za-z0-9])'), ' Administration '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])03186(?![A-Za-z0-9])'), ' HR '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BEGGS(?![A-Za-z0-9])'), ' Administration '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])6999(?![A-Za-z0-9])'), ' Technology '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])KING(?![A-Za-z0-9])'), ' Administration '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])APP(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SECRETARY(?![A-Za-z0-9])'), ' Administration '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ZOOMUS(?![A-Za-z0-9])'), ' Technology '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])8887999666(?![A-Za-z0-9])'), ' Technology '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ZOOM(?![A-Za-z0-9])'), ' Technology '),  # support=15 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FUNDING(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=15 purity=0.9375
    (re.compile(r'(?i)(?<![A-Za-z0-9])25456(?![A-Za-z0-9])'), ' Administration '),  # support=15 purity=0.9375
    (re.compile(r'(?i)(?<![A-Za-z0-9])3999(?![A-Za-z0-9])'), ' Administration '),  # support=15 purity=0.9375
    (re.compile(r'(?i)(?<![A-Za-z0-9])LONG(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=15 purity=0.9375
    (re.compile(r'(?i)(?<![A-Za-z0-9])EVENT(?![A-Za-z0-9])'), ' Marketing '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])USA(?![A-Za-z0-9])'), ' Sublime '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FRONT(?![A-Za-z0-9])'), ' Technology '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])87550(?![A-Za-z0-9])'), ' Technology '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GODADDY(?![A-Za-z0-9])'), ' Technology '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HONEYBUCKET(?![A-Za-z0-9])'), ' Administration '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])2539041324(?![A-Za-z0-9])'), ' Administration '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FRONTIER(?![A-Za-z0-9])'), ' Administration '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])COMM(?![A-Za-z0-9])'), ' Administration '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])4085366000(?![A-Za-z0-9])'), ' Technology '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DELIGHTED(?![A-Za-z0-9])'), ' Technology '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SCZZ(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LOCATIONS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])4ZN2A7PJMYQD(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])240104(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INVOICES(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])44537(?![A-Za-z0-9])'), ' HR '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PICKUP(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CONFIRMED(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PEREZ(?![A-Za-z0-9])'), ' HR '),  # support=14 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MARISA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=0.9333
    (re.compile(r'(?i)(?<![A-Za-z0-9])BADUA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=14 purity=0.9333
    (re.compile(r'(?i)(?<![A-Za-z0-9])IBUDDY(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=14 purity=0.9333
    (re.compile(r'(?i)(?<![A-Za-z0-9])32076(?![A-Za-z0-9])'), ' Sublime '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HUENEME(?![A-Za-z0-9])'), ' Administration '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PAYMENTUS(?![A-Za-z0-9])'), ' Finance '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MUSKRAT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HIGHLAND(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PACAFI(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PCF(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BAKESFIELD(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])60638(?![A-Za-z0-9])'), ' Technology '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])92166(?![A-Za-z0-9])'), ' Administration '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PURCHASE09100001(?![A-Za-z0-9])'), ' Finance '),  # support=13 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])COPAYMENT(?![A-Za-z0-9])'), ' HR '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0663(?![A-Za-z0-9])'), ' Administration '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FLAVORS(?![A-Za-z0-9])'), ' Sublime '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STATIOORINDA(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])RIVERSIDE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MAITLANDM(?![A-Za-z0-9])'), ' Administration '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PATIENT(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ADS3407900941(?![A-Za-z0-9])'), ' Marketing '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])COMMISSION(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])1047(?![A-Za-z0-9])'), ' HR '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])908031047(?![A-Za-z0-9])'), ' HR '),  # support=12 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MACHININPMT(?![A-Za-z0-9])'), ' Sublime '),  # support=12 purity=0.9231
    (re.compile(r'(?i)(?<![A-Za-z0-9])7975(?![A-Za-z0-9])'), ' Technology '),  # support=12 purity=0.9231
    (re.compile(r'(?i)(?<![A-Za-z0-9])SMO(?![A-Za-z0-9])'), ' HR '),  # support=12 purity=0.9231
    (re.compile(r'(?i)(?<![A-Za-z0-9])49000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DUEINTERNET(?![A-Za-z0-9])'), ' HR '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CONES(?![A-Za-z0-9])'), ' Sublime '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CUSTOM(?![A-Za-z0-9])'), ' Sublime '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DELIVERMDBILL(?![A-Za-z0-9])'), ' HR '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PRM(?![A-Za-z0-9])'), ' Sublime '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FILTRATION(?![A-Za-z0-9])'), ' Sublime '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])29480(?![A-Za-z0-9])'), ' HR '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EXXONMOBIL(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])REMOTE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ASHS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INDUS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CELL(?![A-Za-z0-9])'), ' Finance '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WIDE(?![A-Za-z0-9])'), ' Finance '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])30638(?![A-Za-z0-9])'), ' Sublime '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])98462(?![A-Za-z0-9])'), ' Administration '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SLO(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])250(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=11 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MANUAL(?![A-Za-z0-9])'), ' HR '),  # support=11 purity=0.9167
    (re.compile(r'(?i)(?<![A-Za-z0-9])WORLD(?![A-Za-z0-9])'), ' Finance '),  # support=11 purity=0.9167
    (re.compile(r'(?i)(?<![A-Za-z0-9])MENDOZA(?![A-Za-z0-9])'), ' HR '),  # support=11 purity=0.9167
    (re.compile(r'(?i)(?<![A-Za-z0-9])SPENCER(?![A-Za-z0-9])'), ' HR '),  # support=11 purity=0.9167
    (re.compile(r'(?i)(?<![A-Za-z0-9])DELIVERMDDD(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DELIVERMDTAX(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])31864(?![A-Za-z0-9])'), ' Sublime '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SPRINGBIG(?![A-Za-z0-9])'), ' Marketing '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])79590618(?![A-Za-z0-9])'), ' Finance '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])80734(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])6502530000(?![A-Za-z0-9])'), ' Technology '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])RENTAC043488NEWARK(?![A-Za-z0-9])'), 'Inventory - Wholesale'),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])60074(?![A-Za-z0-9])'), ' Technology '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DELIVERMDGPS(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0205456CHEVWESTLEY(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])XXXXXXXXXXXX8630(?![A-Za-z0-9])'), ' Technology '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TIGRANLEVONANDLA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])KIM(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ANTIOCH(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])15500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HEALING(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CENT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])74726(?![A-Za-z0-9])'), ' Administration '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])64000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NETWOR(?![A-Za-z0-9])'), ' Sublime '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])406(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FHL(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WAVE(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INSPMTS(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PREMIU(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NEXT(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DOPE(?![A-Za-z0-9])'), ' Marketing '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])08152023(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NET(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])REM(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PERMIT(?![A-Za-z0-9])'), ' Administration '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])AREA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])39200(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])76000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])TWITCHTVDON(?![A-Za-z0-9])'), ' Finance '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])CONSULTANTS(?![A-Za-z0-9])'), ' Finance '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])SIBANNA(?![A-Za-z0-9])'), ' Finance '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])123(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])20480(?![A-Za-z0-9])'), ' HR '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])01800(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=10 purity=0.9091
    (re.compile(r'(?i)(?<![A-Za-z0-9])HOLDING(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])47600(?![A-Za-z0-9])'), ' HR '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SEMRUSH(?![A-Za-z0-9])'), ' Marketing '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DROPBOX(?![A-Za-z0-9])'), ' Technology '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])READYREFRESHWATERSERV(?![A-Za-z0-9])'), ' Administration '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])READYREFRESH(?![A-Za-z0-9])'), ' Administration '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])7347(?![A-Za-z0-9])'), ' Administration '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NORDHOFF(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])57340(?![A-Za-z0-9])'), ' Finance '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])79000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])83250(?![A-Za-z0-9])'), ' Marketing '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])75604(?![A-Za-z0-9])'), ' HR '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])33306(?![A-Za-z0-9])'), ' Sublime '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MEDIAJEL(?![A-Za-z0-9])'), ' Marketing '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SYS(?![A-Za-z0-9])'), ' HR '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PTB(?![A-Za-z0-9])'), ' HR '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0000000406(?![A-Za-z0-9])'), ' HR '),  # support=9 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HIS2000444(?![A-Za-z0-9])'), ' HR '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])FRANCHISE(?![A-Za-z0-9])'), ' Administration '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])HIS2000461(?![A-Za-z0-9])'), ' HR '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])44000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])0441(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])IMPORT(?![A-Za-z0-9])'), ' Driver Operations - Bay '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])AUTOMOT(?![A-Za-z0-9])'), ' Driver Operations - Bay '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])TARGET(?![A-Za-z0-9])'), ' Administration '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])32000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])615(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])TIRE(?![A-Za-z0-9])'), ' Driver Operations - Bay '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])250617(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=9 purity=0.9
    (re.compile(r'(?i)(?<![A-Za-z0-9])89000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])09012(?![A-Za-z0-9])'), ' Administration '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])33300(?![A-Za-z0-9])'), ' Administration '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CLTV8(?![A-Za-z0-9])'), ' Marketing '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])250415(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INSPREM(?![A-Za-z0-9])'), ' HR '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LICENSING(?![A-Za-z0-9])'), ' Administration '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEMAND(?![A-Za-z0-9])'), ' Marketing '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STEADY(?![A-Za-z0-9])'), ' Marketing '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])09530684(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])3698(?![A-Za-z0-9])'), ' Technology '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MANTECA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MONTEREY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])95252(?![A-Za-z0-9])'), ' HR '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])27697(?![A-Za-z0-9])'), ' HR '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])92666(?![A-Za-z0-9])'), ' Administration '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ONBOARD(?![A-Za-z0-9])'), ' Executive '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SMOKELAND(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])8558726632(?![A-Za-z0-9])'), ' Driver Operations - Sacramento '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])68880(?![A-Za-z0-9])'), ' Technology '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MEGANS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])41500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SINGLE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PRIME1(?![A-Za-z0-9])'), ' Technology '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CKO(?![A-Za-z0-9])'), ' Technology '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])70833(?![A-Za-z0-9])'), ' HR '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])YOUNG(?![A-Za-z0-9])'), ' HR '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TECHNO(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ARIEL(?![A-Za-z0-9])'), ' HR '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MUNOZ(?![A-Za-z0-9])'), ' HR '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])AVISNYCFEE(?![A-Za-z0-9])'), 'Inventory - Wholesale'),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HEART(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PUREST(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])XXXXXXXXXXXX7706(?![A-Za-z0-9])'), ' Technology '),  # support=8 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HIS2000442(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])250408(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])AARON(?![A-Za-z0-9])'), ' Administration '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SHAY(?![A-Za-z0-9])'), ' Administration '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])41393(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])77280(?![A-Za-z0-9])'), ' Technology '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])091000010000001(?![A-Za-z0-9])'), ' Finance '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACCELA(?![A-Za-z0-9])'), ' Technology '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PASAN(?![A-Za-z0-9])'), 'Inventory - Wholesale'),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])60776SFO(?![A-Za-z0-9])'), 'Inventory - Wholesale'),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])3499(?![A-Za-z0-9])'), ' Technology '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])8778601258(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])UNLTD(?![A-Za-z0-9])'), ' Technology '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])KINDLE(?![A-Za-z0-9])'), ' Technology '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])57000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CORONA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INDIO(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SENDER(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])60500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])OYAMASUSH(?![A-Za-z0-9])'), ' Administration '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ROCKET(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])83666(?![A-Za-z0-9])'), ' Administration '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])56500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])11705(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DELIVERMDSMOAKLAND(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])44500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0430(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])53192(?![A-Za-z0-9])'), ' Sublime '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WILTON(?![A-Za-z0-9])'), ' Sublime '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0401(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0301(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])23267(?![A-Za-z0-9])'), ' Sublime '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0201(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EARTHWISE(?![A-Za-z0-9])'), ' Sublime '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])82113(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])08072023(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACHGUSTO(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PERMANENTE(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])8008336687(?![A-Za-z0-9])'), ' Technology '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SHOPIFY(?![A-Za-z0-9])'), ' Technology '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])JPS(?![A-Za-z0-9])'), ' Administration '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MARTINEZ(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])1995(?![A-Za-z0-9])'), ' Marketing '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SENDGRID(?![A-Za-z0-9])'), ' Marketing '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MELISSA(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PAYROLLSYS(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])XFINITY(?![A-Za-z0-9])'), ' Administration '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BAKED(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FRESHLY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NYC(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TRADING(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NSEW(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DISPE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])F54C43DBBDB10000000(?![A-Za-z0-9])'), ' HR '),  # support=7 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ELHSBCHKHHHKH(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PORTER(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])JATO(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FIXED(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])IMPROVEMENTS(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ASSETS(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LEASEHOLD(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])19134(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ANA(?![A-Za-z0-9])'), ' Marketing '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MOLDS(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])27696(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INCINS(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])15590(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])15589(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GILMOR(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HDN(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EXTRACTION(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PRECISION(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MODERNIST(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PANTRY(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])8669094458(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])93217(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])INTERESTHUMAN(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CORPORATEFEE(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EVERON(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FRAGRANCE(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])18995(?![A-Za-z0-9])'), ' Marketing '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])13846(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])STATIOCASTAIC(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])8499(?![A-Za-z0-9])'), ' Technology '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ADMINPSADMINIST(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])YELLOWIMAGESCOM(?![A-Za-z0-9])'), ' Technology '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])YELLOWIMAGES(?![A-Za-z0-9])'), ' Technology '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])814(?![A-Za-z0-9])'), ' Finance '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FWEB(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ISLAMIC(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])45500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])47000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])74000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])85500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])90300(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])95500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])62400(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BANYAN(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])59200(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TST(?![A-Za-z0-9])'), ' Executive '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SAIGONHOU(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])56296(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])14500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])94000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FINANCING(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PFS(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])X1182(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])06000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])92401(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0731(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])05500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])13500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ANT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0531(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TIME(?![A-Za-z0-9])'), ' Technology '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FAX(?![A-Za-z0-9])'), ' Technology '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0601(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])74500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BLV(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0101(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LTD(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TARGETCOM(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])39735(?![A-Za-z0-9])'), ' Administration '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACHNORTH(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PPD(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MICHAEL(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BARR(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])IRMA(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MENDOZAPEREZ(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GOMES(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LEONIDAS(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PROCESSING(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])OBATA(?![A-Za-z0-9])'), ' HR '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])67172(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WEHO(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FLORATERRA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CHO(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FLORA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SONOMA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])39703(?![A-Za-z0-9])'), ' Sublime '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GUARDIAN(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=6 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])72800(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HONOR(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TENSTRIKE(?![A-Za-z0-9])'), ' Marketing '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])21305(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SOLUTIONSSALE(?![A-Za-z0-9])'), ' Technology '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NYS(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])88687(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GROWTH(?![A-Za-z0-9])'), ' Technology '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])GROWTH2(?![A-Za-z0-9])'), ' Technology '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MARKETPARMOREDCAR(?![A-Za-z0-9])'), ' Finance '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FORMATION(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])8009378997(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])47256(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])38024(?![A-Za-z0-9])'), ' Sublime '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PUBLISHING(?![A-Za-z0-9])'), ' Marketing '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])METRO(?![A-Za-z0-9])'), ' Marketing '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0097407CHEVORINDA(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0230(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ARCBUTTONWILLOW(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])00000WESTLEY(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MICROSOFT(?![A-Za-z0-9])'), ' Technology '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WINGSTOP(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])HILIFE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])THIRD(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])250502(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ARTESIA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])42400(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ACDFEE(?![A-Za-z0-9])'), ' Finance '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])17200(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])CALITA(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PYMTS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WELLNES(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])RETA(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])72500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DEPOSITS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MAIL(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SHENZHEN(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])17582(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])SAEFONG(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])54372(?![A-Za-z0-9])'), ' Sublime '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])JOHNSON(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])JAVEN(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])84500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])32769(?![A-Za-z0-9])'), ' Sublime '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FROOT(?![A-Za-z0-9])'), ' Inventory - Wholesale '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])2217(?![A-Za-z0-9])'), ' Technology '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0630(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])39000(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])95200(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0702(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0607(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0606(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0605(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0529(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0527(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0522(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0520(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0521(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0518(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0515(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0513(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0511(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])97500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0509(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0508(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0503(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0502(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0501(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])10847(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0426(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MARTKWIK(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])ZIPPY(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EXTRACT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0420(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0421(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PAYMENTS(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WEBSTAURANT(?![A-Za-z0-9])'), ' Sublime '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0418(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])WPY(?![A-Za-z0-9])'), ' Marketing '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0417(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0406(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0407(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0403(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0404(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0331(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0402(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0328(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0322(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0321(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0320(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0317(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0318(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0316(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0314(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0313(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0312(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0311(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0308(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0305(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0306(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0303(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0304(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FOUR(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])3523(?![A-Za-z0-9])'), 'Driver Operations - Bay'),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0226(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0222(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0221(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0218(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0216(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0215(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0214(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0213(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0212(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0208(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BLOOM(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0205(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0206(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0131(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0203(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0204(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0202(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0129(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0128(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0126(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0122(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])PIN(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0117(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0115(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0114(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0112(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0111(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])1231(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0102(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])0103(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])84112(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])3QHK(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])50089(?![A-Za-z0-9])'), ' Sublime '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])78500(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])LZC(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NICE(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MARIA(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])KANDY(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])YENI(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])MORALES(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BRIANA(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])86589(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])280301001875(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])91327(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])18660(?![A-Za-z0-9])'), ' Technology '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EUNOIA(?![A-Za-z0-9])'), ' Administration '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NORTHBAY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])BILLPAY(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])FINANCIALS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])DELIVERMDTRANSFER(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])13600(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])NUCLEUS(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EXIT(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])EXOTIC(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])18697(?![A-Za-z0-9])'), ' Executive '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])TWENTY8GRAMZ(?![A-Za-z0-9])'), ' NO DEPARTMENT '),  # support=5 purity=1.0
    (re.compile(r'(?i)(?<![A-Za-z0-9])250903(?![A-Za-z0-9])'), ' HR '),  # support=5 purity=1.0
]

# Fields concatenated to build the searchable text.
# Order matters; we include outputs of previous rulebooks to simplify patterns.
_SOURCE_COLS: List[str] = [
    "bank_account", "subentity", "bank_cc_num",
    "payee_vendor", "cf_account",
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
