# src/parsers/credit_card.py
from __future__ import annotations
import os, re
from typing import Optional, Dict, Any
import pandas as pd

from .base import Parser, StandardizedRecord
from . import utils


# --- filename -> (bank_account, bank_cc_num) -------------------------------
# Rules provided:
#   "CC 9551"  -> bank_account="SUB", bank_cc_num="9551"
#   "CC 8267"  -> bank_account="TPH", bank_cc_num="8267"
#   "CC 71000" -> bank_account="DMD", bank_cc_num="71000"  (special: invert signs)
#   "CC 7639"  -> bank_account="TPH", bank_cc_num="7639"
def _infer_from_filename(path: str) -> Dict[str, str]:
    name = os.path.basename(path).lower()
    if "9551" in name:
        return {"bank_account": "SUB", "bank_cc_num": "9551", "invert": False}
    if "8267" in name:
        return {"bank_account": "TPH", "bank_cc_num": "8267", "invert": False}
    if "71000" in name:
        return {"bank_account": "DMD", "bank_cc_num": "71000", "invert": True}
    if "7639" in name:
        return {"bank_account": "TPH", "bank_cc_num": "7639", "invert": False}

    # Fallback: try to grab last 4/5 digits; default account label "CC"
    m = re.search(r"(\d{4,5})", name)
    last = m.group(1) if m else ""
    return {"bank_account": "CC", "bank_cc_num": last, "invert": False}


class CreditCardParser(Parser):
    """
    Handles multiple CC CSV layouts, e.g.:
    - Date, Receipt, Description, Card Member, Account #, Amount
    - Status, Date, Description, Debit, Credit
    - Card, Transaction Date, Post Date, Description, Category, Type, Amount, Memo
    """

    def parse(
        self,
        *,
        file_path: str,
        df: pd.DataFrame,
        ctx: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        ctx = ctx or {}

        # Infer account metadata from filename (ctx can override)
        inferred = _infer_from_filename(file_path)
        bank_account = ctx.get("bank_account") or inferred["bank_account"]
        bank_cc_num  = ctx.get("bank_cc_num")  or inferred["bank_cc_num"]
        subentity    = ctx.get("subentity", "")
        invert_signs = bool(ctx.get("invert", inferred["invert"]))

        # Resolve headers (case-insensitive)
        date_c    = self._col(df, ["Post Date", "Transaction Date", "Date"])
        desc_c    = self._col(df, ["Description", "Memo"])          # main text
        memo_c    = self._col(df, ["Memo"])                         # optional 2nd text
        cat_c     = self._col(df, ["Category"])                     # used for 9551/8267 rule
        acctnum_c = self._col(df, ["Account #", "Account Number", "Account", "Card Number", "Card #"])  # 71000 rule
        amt_c     = self._col(df, ["Amount"])
        debit_c   = self._col(df, ["Debit"])
        credit_c  = self._col(df, ["Credit"])
        bal_c     = self._col(df, ["Balance", "Running Balance"])
        id_c      = self._col(df, ["Transaction ID", "Reference ID", "ID"])

        n = len(df)

        # ----- Normalize amount --------------------------------------------
        # Uniform rule for all cards:
        if amt_c:
            amount_s = utils.clean_amount_series(df[amt_c])
        else:
            debit_s  = utils.clean_amount_series(df[debit_c]) if debit_c  else pd.Series([0]*n)
            credit_s = utils.clean_amount_series(df[credit_c]) if credit_c else pd.Series([0]*n)
            amount_s = credit_s.abs().fillna(0) - debit_s.abs().fillna(0)

        # Special: cards that come inverted (71000 per infer) → flip signs
        if invert_signs and amount_s is not None:
            amount_s = -amount_s

        balance_s = utils.clean_amount_series(df[bal_c]) if bal_c else pd.Series([None]*n)

        # Transaction ID: prefer bank-provided; else make a stable hash
        if id_c:
            txn_id_s = df[id_c].astype(str)
        else:
            anchor = df[desc_c] if desc_c else pd.Series([""]*n)
            txn_id_s = utils.stable_txn_id(
                bank_account=bank_account,
                bank_cc_num=bank_cc_num,
                date_val=df[date_c] if date_c else pd.Series([None]*n),
                amount_val=amount_s,
                description_val=anchor,
                row_index=df.index.to_series(),
            )

        # ----- Description / extended_description rules ---------------------
        # Base description: prefer Description, fallback to Memo
        base_desc_s = (
            df[desc_c].astype(str)
            if desc_c else (df[memo_c].astype(str) if memo_c else pd.Series([""]*n))
        )

        # Compute extended_description per special rules
        ext_desc_s = pd.Series([None]*n, dtype=object)

        if bank_cc_num in {"9551", "8267"}:
            # Category -> extended_description
            if cat_c:
                ext_desc_s = df[cat_c].astype(str).where(df[cat_c].notna(), None)
        elif bank_cc_num in {"71000", "7101"}:
            # Account # -> extended_description
            if acctnum_c:
                ext_desc_s = df[acctnum_c].astype(str).where(df[acctnum_c].notna(), None)
        else:
            # Default behavior: if there is a second text field (Memo), use pick_description
            if memo_c and desc_c:
                pairs = [
                    utils.pick_description(d, m)
                    for d, m in zip(df[desc_c].astype(str), df[memo_c].astype(str))
                ]
                base_desc_s = pd.Series([p[0] for p in pairs], dtype=object)
                ext_desc_s  = pd.Series([p[1] for p in pairs], dtype=object)
            # If only one text column existed, ext_desc_s stays None

        # ----- Build rows ---------------------------------------------------
        rows = []
        for i, r in df.iterrows():
            rows.append(
                StandardizedRecord(
                    bank_source="CC",
                    bank_account=bank_account,
                    subentity=subentity,
                    bank_cc_num=bank_cc_num,
                    txn_id=str(txn_id_s.iloc[i]),
                    date=pd.to_datetime(r.get(date_c), errors="coerce") if date_c else pd.NaT,
                    amount=(float(amount_s.iloc[i]) if pd.notna(amount_s.iloc[i]) else None),
                    description=str(base_desc_s.iloc[i]),
                    extended_description=(ext_desc_s.iloc[i] if pd.notna(ext_desc_s.iloc[i]) else None),
                    balance=(float(balance_s.iloc[i]) if pd.notna(balance_s.iloc[i]) else None),
                )
            )

        return self._build_df(rows)
