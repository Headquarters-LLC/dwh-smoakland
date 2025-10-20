# src/parsers/base.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import date as DateType
import pandas as pd

# Final, standardized columns every parser must output
STANDARD_COLUMNS = [
    "bank_source",     # origin label: "keypoint", "eastwest", "credit_card", etc.
    "bank_account",    # high-level account label (e.g., TPH, SUB) for reporting
    "subentity",       # optional sub-tag; keep empty string if unused
    "bank_cc_num",     # last4 (or stable identifier) of the bank/cc account
    "txn_id",          # stable transaction id (source id or hashed surrogate)
    "date",            # Date
    "amount",          # float (negative = outflow)
    "description",     # longer text/extended description
    "extended_description", # optional longer text if available
    "balance",         # optional running balance (float) if available
]

@dataclass
class StandardizedRecord:
    bank_source: str
    bank_account: str
    subentity: str
    bank_cc_num: str
    txn_id: str
    date: DateType
    amount: float
    description: Optional[str] = None
    extended_description: Optional[str] = None
    balance: Optional[float] = None

    def to_row(self) -> Dict[str, Any]:
        """Coerce types and return a plain dict honoring STANDARD_COLUMNS."""
        d = asdict(self)
        ts = pd.to_datetime(d["date"], errors="coerce")
        d["date"] = (None if pd.isna(ts) else ts.date())
        d["amount"] = pd.to_numeric(d["amount"], errors="coerce")
        if d.get("balance") is not None:
            d["balance"] = pd.to_numeric(d["balance"], errors="coerce")
        # Ensure all string-like optional fields are strings (or empty) for consistency
        for k in ("bank_account", "subentity", "bank_cc_num", "description"):
            v = d.get(k)
            d[k] = "" if v is None else str(v)
        return d

class Parser(ABC):
    @abstractmethod
    def parse(
        self,
        df: pd.DataFrame,
        ctx: Optional[Dict[str, Any]] = None,
        file_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """Return a DataFrame with STANDARD_COLUMNS."""

    @staticmethod
    def _col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
        """Find the first matching column (case-insensitive)."""
        low = {c.lower(): c for c in df.columns}
        for c in candidates:
            if c.lower() in low:
                return low[c.lower()]
        return None

    def _build_df(self, rows: List[StandardizedRecord]) -> pd.DataFrame:
        """Build a DataFrame in the canonical column order (even if empty)."""
        if not rows:
            return pd.DataFrame(columns=STANDARD_COLUMNS)
        out = pd.DataFrame([r.to_row() for r in rows], columns=STANDARD_COLUMNS)
        return out
