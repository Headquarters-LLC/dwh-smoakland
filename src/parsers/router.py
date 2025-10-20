# src/parsers/router.py
from __future__ import annotations
import os, re
import pandas as pd
from .base import Parser
from .keypoint import KeyPointParser
from .credit_card import CreditCardParser
from .eastwest import EastWestParser
from .nbcu import NBCUParser
from .dama import DamaParser
from .generic import GenericParser

# Optional: keep signatures around (disabled below)
KEYPOINT_SIGNATURE   = {"transaction id", "description", "amount", "balance"}
CREDITCARD_SIGNATURE = {"date", "description", "amount"}
EASTWEST_SIGNATURE   = {"account id", "transaction id", "category"}
NBCU_SIGNATURE       = {"posting date", "transaction date", "amount"}
DAMA_SIGNATURE       = {"id", "description", "debit", "credit"}

# --- helpers -----------------------------------------------------------------

def _norm_name(path: str) -> str:
    """Uppercase, remove extension, collapse separators to single spaces."""
    base = os.path.basename(path)
    stem = os.path.splitext(base)[0]
    stem = stem.upper()
    stem = re.sub(r"[_\-\s\.]+", " ", stem).strip()
    return stem  # e.g., "EWB 3439", "CC 9551", "KP 6852", "NBCU 2211", "DAMA 5597"

def _contains_keyword(stem: str, keyword: str) -> bool:
    """True if the normalized name contains the keyword as a whole word."""
    return re.search(rf"\b{re.escape(keyword)}\b", stem) is not None

# --- main API ----------------------------------------------------------------

def detect_source(file_path: str, df_head: pd.DataFrame) -> str:
    """
    Decide ONLY by normalized filename. Match is 'contains' (whole word), not 'startswith'.
    Example matches: '.../EWB 3439.csv' -> eastwest, '.../CC 8267.CSV' -> credit_card.
    """
    stem = _norm_name(file_path)

    if _contains_keyword(stem, "CC"):
        return "credit_card"
    if _contains_keyword(stem, "KP"):
        return "keypoint"
    if _contains_keyword(stem, "EWB"):
        return "eastwest"
    if _contains_keyword(stem, "NBCU"):
        return "nbcu"
    if _contains_keyword(stem, "DAMA"):
        return "dama"

    # --- Optional fallback by column signature (disabled by request) ----------
    # cols = {c.strip().lower() for c in df_head.columns}
    # if KEYPOINT_SIGNATURE.issubset(cols):   return "keypoint"
    # if CREDITCARD_SIGNATURE.issubset(cols): return "credit_card"
    # if EASTWEST_SIGNATURE.issubset(cols):   return "eastwest"
    # if NBCU_SIGNATURE.issubset(cols):       return "nbcu"
    # if DAMA_SIGNATURE.issubset(cols):       return "dama"

    # Last resort
    return "generic"

def get_parser(kind: str) -> Parser:
    if kind == "keypoint":
        return KeyPointParser()
    if kind == "credit_card":
        return CreditCardParser()
    if kind == "eastwest":
        return EastWestParser()
    if kind == "nbcu":
        return NBCUParser()
    if kind == "dama":
        return DamaParser()
    return GenericParser()
