# src/parsers/nbcu.py
from __future__ import annotations
import os, re
from typing import Optional, Dict, Any
import pandas as pd

from .base import Parser, StandardizedRecord
from . import utils


# --- filename -> (bank_account, bank_cc_num) -------------------------------
# Rules:
#   "NBCU SWD"  -> bank_account="SWD", bank_cc_num="SWD"
#   "NBCU 2211" -> bank_account="SUB", bank_cc_num="2211"
#   "NBCU 2035" -> bank_account="DMD", bank_cc_num="2035"
def _infer_from_filename(path: str) -> Dict[str, str]:
    name = os.path.basename(path).lower()
    if "swd" in name:
        return {"bank_account": "SWD", "bank_cc_num": "SWD"}
    if "2211" in name:
        return {"bank_account": "SUB", "bank_cc_num": "2211"}
    if "2035" in name:
        return {"bank_account": "DMD", "bank_cc_num": "2035"}

    # fallback: try extracting last 4 digits from filename
    m4 = re.search(r"(\d{4})", name)
    last4 = m4.group(1) if m4 else ""
    return {"bank_account": "NBCU", "bank_cc_num": last4}


class NBCUParser(Parser):
    """
    Handles NBCU CSV exports.

    Expected headers:
      Posting Date / Date
      Description / Memo
      Amount  (or separate Debit / Credit columns)
      Balance / Running Balance
      Transaction ID / Reference / ID

    Important: mapping for bank_account and bank_cc_num is taken from the
    *filename*, not the file contents.
    """

    def parse(
        self,
        *,
        file_path: str,
        df: pd.DataFrame,
        ctx: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        ctx = ctx or {}

        # Map from filename
        inferred = _infer_from_filename(file_path)
        bank_account = ctx.get("bank_account") or inferred["bank_account"]
        bank_cc_num  = ctx.get("bank_cc_num")  or inferred["bank_cc_num"]
        subentity    = ctx.get("subentity", "")

        # Resolve headers (case-insensitive)
        date_c   = self._col(df, ["Posting Date", "Date"])
        desc_c   = self._col(df, ["Description", "Memo"])
        amt_c    = self._col(df, ["Amount"])
        debit_c  = self._col(df, ["Debit"])
        credit_c = self._col(df, ["Credit"])
        bal_c    = self._col(df, ["Balance", "Running Balance"])
        id_c     = self._col(df, ["Transaction ID", "Reference", "ID"])
        check_c   = self._col(df, ["Check Number", "CheckNumber", "Check Num", "Check #"])

        # Normalize amount
        if amt_c:
            amount_s = utils.clean_amount_series(df[amt_c])
        else:
            debit_s  = utils.clean_amount_series(df[debit_c])  if debit_c  else pd.Series([0]*len(df))
            credit_s = utils.clean_amount_series(df[credit_c]) if credit_c else pd.Series([0]*len(df))
            # convention: amount = credit - debit
            amount_s = credit_s.fillna(0) - debit_s.fillna(0)
            # if both were missing, you can mask back to NaN if desired:
            # both_missing = debit_s.isna() & credit_s.isna()
            # amount_s = amount_s.mask(both_missing)

        balance_s = utils.clean_amount_series(df[bal_c]) if bal_c else pd.Series([None]*len(df))

        # Transaction ID: prefer bank-provided, otherwise build stable hash
        if id_c:
            txn_id_s = df[id_c].astype(str)
        else:
            desc_anchor = df[desc_c] if desc_c else pd.Series([""]*len(df))
            txn_id_s = utils.stable_txn_id(
                bank_account=bank_account,
                bank_cc_num=bank_cc_num,
                date_val=df[date_c] if date_c else pd.Series([None]*len(df)),
                amount_val=amount_s,
                description_val=desc_anchor,
                row_index=df.index.to_series(),
            )

        # Build standardized rows
        rows = []
        for i, r in df.iterrows():
            # Description is the normal description/memo text
            description = str(r.get(desc_c, "") or "") if desc_c else ""

            # Rule requested: Check Number -> extended_description
            ext_desc = None
            if check_c:
                val = str(r.get(check_c, "") or "").strip()
                ext_desc = val if val else None

            rows.append(
                StandardizedRecord(
                    bank_source="NBCU",
                    bank_account=bank_account,
                    subentity=subentity,
                    bank_cc_num=bank_cc_num,
                    txn_id=str(txn_id_s.iloc[i]),
                    date=pd.to_datetime(r.get(date_c), errors="coerce") if date_c else pd.NaT,
                    amount=(float(amount_s.iloc[i]) if pd.notna(amount_s.iloc[i]) else None),
                    description=description,
                    extended_description=ext_desc,
                    balance=(float(balance_s.iloc[i]) if pd.notna(balance_s.iloc[i]) else None),
                )
            )

        return self._build_df(rows)
