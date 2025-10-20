# src/parsers/eastwest.py
from __future__ import annotations

import os
import re
from typing import Optional, Dict, Any
import pandas as pd

from .base import Parser, StandardizedRecord
from . import utils

# --- Account mapping (ID/Nickname -> bank_account) ---
EWB_ID_TO_ACCOUNT = {
    "8420003447": "DMD",
    "8420003439": "TPH",
    "8420018452": "SSC",
}
EWB_NICK_TO_ACCOUNT = {
    "smoakland": "DMD",
    "the purest heart 786, inc": "TPH",
    "smoakland supply corp": "SSC",
}

def _infer_from_filename(path: str) -> Dict[str, str]:
    """Infer last4 from filename; default bank_account label 'EWB'."""
    name = os.path.basename(path).lower()
    m = re.search(r"(\d{4})", name)
    return {
        "bank_account": "EWB",
        "bank_cc_num": m.group(1) if m else "",
        "subentity": "",
    }

class EastWestParser(Parser):
    """
    Expected headers seen in samples:
      AccountID | AccountNickname | Date | Transaction | Description | Status | DebitCredit | Balance
    Also supports variants like: Posting Date / Transaction Date, Transaction Amount, Running Balance, Reference Number.
    """

    def parse(
        self,
        *,
        file_path: str,
        df: pd.DataFrame,
        ctx: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        ctx = ctx or {}
        inferred = _infer_from_filename(file_path)

        # Resolve columns (case-insensitive)
        date_c = self._col(df, ["Date", "Posting Date", "Transaction Date"])
        trans_c = self._col(df, ["Transaction"])
        desc_c = self._col(df, ["Description", "Transaction Description"])
        # Some exports put sign in "DebitCredit" or "Amount"/"Transaction Amount"
        amt_c  = self._col(df, ["Amount", "Transaction Amount", "DebitCredit"])
        bal_c  = self._col(df, ["Balance", "Running Balance"])
        id_c   = self._col(df, ["Transaction ID", "Reference Number", "Reference"])
        acct_c = self._col(df, ["Account Number", "AccountID", "Account"])
        nick_c = self._col(df, ["AccountNickname", "Account Nickname", "Nickname"])

        # ----- Map bank_account (priority: ctx > by ID > by nickname > inferred/default) -----
        acct_id_first = ""
        if acct_c and len(df):
            acct_id_first = str(df[acct_c].iloc[0]).strip()
        acct_id_digits = re.sub(r"\D", "", acct_id_first)

        nick_first = ""
        if nick_c and len(df):
            nick_first = str(df[nick_c].iloc[0]).strip().lower()

        bank_account = str(
            ctx.get("bank_account")
            or EWB_ID_TO_ACCOUNT.get(acct_id_digits)
            or EWB_NICK_TO_ACCOUNT.get(nick_first)
            or inferred["bank_account"]
            or "EWB"
        )
        bank_cc_num = str(
            ctx.get("bank_cc_num")
            or inferred["bank_cc_num"]
            or (acct_id_digits[-4:] if acct_id_digits else "")
        )
        subentity = str(ctx.get("subentity") or inferred.get("subentity") or "")

        # ----- Clean numerics -----
        df["_amount_clean"]  = utils.clean_amount_series(df[amt_c]) if amt_c else pd.Series([None] * len(df))
        df["_balance_clean"] = utils.clean_amount_series(df[bal_c]) if bal_c else pd.Series([None] * len(df))

        # ----- txn_id (prefer provided; else deterministic hash) -----
        if id_c:
            df["_txn_id"] = df[id_c].astype(str)
        else:
            df["_txn_id"] = utils.stable_txn_id(
                bank_account=bank_account,
                bank_cc_num=bank_cc_num,
                date_val=df[date_c] if date_c else None,
                amount_val=df["_amount_clean"],
                description_val=df[desc_c] if desc_c else "",
                row_index=df.index.to_series(),
            )

        # Last4 per row (try from column; fallback to inferred)
        if acct_c:
            last4_series = (
                df[acct_c].astype(str).str.extract(r"(\d{4})$")[0]
                .fillna(bank_cc_num).astype(str)
            )
        else:
            last4_series = pd.Series([bank_cc_num] * len(df))

        # ----- Build standardized rows (no 'vendor' in core) -----
        rows = []
        for i, r in df.iterrows():
            if trans_c and desc_c:
                description = str(r.get(trans_c, "") or "")
                extended_description = str(r.get(desc_c, "") or "") or None
            elif trans_c:
                description = str(r.get(trans_c, "") or "")
                extended_description = None
            elif desc_c:
                description = str(r.get(desc_c, "") or "")
                extended_description = None
            else:
                description = ""
                extended_description = None

            rows.append(
                StandardizedRecord(
                    bank_source="EWB",
                    bank_account=bank_account,
                    subentity=subentity,
                    bank_cc_num=last4_series.iloc[i],
                    txn_id=r["_txn_id"],
                    date=pd.to_datetime(r.get(date_c), errors="coerce") if date_c else pd.NaT,
                    amount=r.get("_amount_clean"),
                    description=description,
                    extended_description=extended_description,
                    balance=r.get("_balance_clean"),
                )
            )

        return self._build_df(rows)
