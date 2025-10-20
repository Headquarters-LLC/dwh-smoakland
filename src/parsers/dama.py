# src/parsers/dama.py
from __future__ import annotations
import os, re
from typing import Optional, Dict, Any
import pandas as pd

from .base import Parser, StandardizedRecord
from . import utils

# ---- Dictionaries (normalized to lowercase for robust comparison) ----
# client name -> (bank_account, subentity)
DAMA_CLIENT_MAP: dict[str, tuple[str, str]] = {
    "bb & se inc": ("TPH", "SaCal"),
    "smoakland":   ("TPH", "NY"),
}

# last4 -> (bank_account, subentity)
# Priority mapping when bank_cc_num is known
DAMA_LAST4_MAP: dict[str, tuple[str, str]] = {
    "5597": ("TPH", "SaCal"),
    "7403": ("TPH", "NY"),
}

def _last4_from_text(val: Any) -> str:
    """Return the last 4 digits present in the text, preferring standalone 4-digit groups.
    If no standalone group is found, strip non-digits and take the last 4 digits.
    """
    s = str(val) if val is not None else ""
    matches = list(re.finditer(r"(?<!\d)(\d{4})(?!\d)", s))
    if matches:
        return matches[-1].group(1)
    digits = re.sub(r"\D+", "", s)
    return digits[-4:] if len(digits) >= 4 else ""

def _infer_from_filename(path: str) -> Dict[str, str]:
    """Fallback metadata derived from the filename.
    bank_account defaults to 'DAMA'; bank_cc_num is extracted as last4 from filename.
    """
    name = os.path.basename(path)
    return {
        "bank_account": "DAMA",
        "bank_cc_num": _last4_from_text(name),
        "subentity": "",
    }

class DamaParser(Parser):
    """
    Typical headers in Dama exports:
      Client Name | Wallet Number | Transaction ID | Date | Parent Type | Transaction Reference
      Type | Partner Name | Status | Debit | Credit | Balance | CheckNumber | Memo
    """

    def parse(
        self,
        *,
        file_path: str,
        df: pd.DataFrame,
        ctx: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        ctx = ctx or {}

        # --- Resolve headers case-insensitively ---
        date_c    = self._col(df, ["Date"])
        memo_c    = self._col(df, ["Memo", "Description"])  # Dama uses "Memo" instead of "Description"
        debit_c   = self._col(df, ["Debit"])
        credit_c  = self._col(df, ["Credit"])
        bal_c     = self._col(df, ["Balance"])
        tid_c     = self._col(df, ["Transaction ID", "ID"])
        ref_c     = self._col(df, ["Transaction Reference", "Reference"])
        wallet_c  = self._col(df, ["Wallet Number", "Account", "Account Number"])
        client_c  = self._col(df, ["Client Name", "Entity"])
        type_c    = self._col(df, ["Type"])

        # --- Fallbacks from filename (low priority) ---
        inferred = _infer_from_filename(file_path)

        # --- Normalize first client name (for optional client-based mapping) ---
        client_first = (
            df[client_c].astype(str).str.strip().str.lower().iloc[0]
            if client_c and len(df) > 0 else ""
        )
        mapped_by_client = DAMA_CLIENT_MAP.get(client_first)

        # --- Resolve bank_cc_num (last4) ASAP to enable mapping by last4 ---
        wallet_val = (
            str(df[wallet_c].iloc[0]) if wallet_c and len(df) > 0 else ""
        )
        bank_cc_num = str(
            ctx.get("bank_cc_num")
            or _last4_from_text(wallet_val)
            or inferred["bank_cc_num"]
        )

        # --- Resolve bank_account and subentity with clear priority ---
        # 1) ctx overrides
        # 2) mapping by last4 (DAMA_LAST4_MAP)
        # 3) mapping by client name (DAMA_CLIENT_MAP)
        # 4) filename fallback
        mapped_by_last4 = DAMA_LAST4_MAP.get(bank_cc_num) if bank_cc_num else None

        bank_account = str(
            ctx.get("bank_account")
            or (mapped_by_last4[0] if mapped_by_last4 else (mapped_by_client[0] if mapped_by_client else inferred["bank_account"]))
        )
        subentity = str(
            ctx.get("subentity")
            or (mapped_by_last4[1] if mapped_by_last4 else (mapped_by_client[1] if mapped_by_client else inferred.get("subentity", "")))
        )

        # --- Optional: warn if client-based mapping conflicts with last4-based mapping ---
        if mapped_by_last4 and mapped_by_client and (mapped_by_last4 != mapped_by_client):
            # Prefer last4 mapping but emit a warning for visibility
            if hasattr(self, "logger") and self.logger:
                self.logger.warning({
                    "file": file_path,
                    "warning": "Mismatch between client map and last4 map; last4 mapping is preferred.",
                    "client_first": client_first,
                    "mapped_client": mapped_by_client,
                    "mapped_last4": mapped_by_last4,
                    "bank_cc_num": bank_cc_num,
                    "wallet_val": wallet_val or None,
                })

        # --- Normalize numerics ---
        debit_s  = utils.clean_amount_series(df[debit_c])  if debit_c  else pd.Series([None]*len(df))
        credit_s = utils.clean_amount_series(df[credit_c]) if credit_c else pd.Series([None]*len(df))
        bal_s    = utils.clean_amount_series(df[bal_c])    if bal_c    else pd.Series([None]*len(df))

        # amount = credit - debit  (treat missing as 0)
        amount_s = (credit_s.fillna(0) - debit_s.fillna(0))

        # --- txn_id: prefer Transaction ID, fallback to Reference, else stable hash ---
        if tid_c:
            txn_id_s = df[tid_c].astype(str)
        elif ref_c:
            txn_id_s = df[ref_c].astype(str)
        else:
            txn_id_s = utils.stable_txn_id(
                bank_account=bank_account,
                bank_cc_num=bank_cc_num,
                date_val=(df[date_c] if date_c else pd.Series([None]*len(df))),
                amount_val=amount_s,
                description_val=(df[memo_c] if memo_c else pd.Series([""]*len(df))),
                row_index=df.index.to_series(),
            )

        # --- Build standardized rows ---
        rows = []
        for i, r in df.iterrows():
            # Use "Memo" (or "Description") as the primary description text
            description = str(r.get(memo_c, "") or "") if memo_c else ""

            # Requested rule: put "Type" into extended_description (if present)
            ext_desc = None
            if type_c:
                val = str(r.get(type_c, "") or "").strip()
                ext_desc = val if val else None

            rows.append(
                StandardizedRecord(
                    bank_source="DAMA",
                    bank_account=bank_account,
                    subentity=subentity,
                    bank_cc_num=bank_cc_num,  # same last4 for all rows in this file
                    txn_id=str(txn_id_s.iloc[i]),
                    date=pd.to_datetime(r.get(date_c), errors="coerce") if date_c else pd.NaT,
                    amount=(float(amount_s.iloc[i]) if pd.notna(amount_s.iloc[i]) else None),
                    description=description,
                    extended_description=ext_desc,
                    balance=(float(bal_s.iloc[i]) if pd.notna(bal_s.iloc[i]) else None),
                )
            )
        return self._build_df(rows)
