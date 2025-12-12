# src/parsers/keypoint.py
from __future__ import annotations
import os, re
from typing import Optional, Dict, Any
import pandas as pd
from .base import Parser, StandardizedRecord
from . import utils

KP_ID_TO_ACCOUNT = {"528306852": "HAH"}
KP_NICK_TO_ACCOUNT = {"smoakland": "HAH"}

def _last4_from(val: Any) -> str:
    s = str(val) if val is not None else ""
    m = re.search(r"(\d{4})$", s)
    return m.group(1) if m else ""

def _infer_from_filename(path: str) -> Dict[str, str]:
    name = os.path.basename(path).lower()
    m4 = re.search(r"(\d{4})", name)
    last4 = m4.group(1) if m4 else ""
    return {"bank_account": "KP", "bank_cc_num": last4, "subentity": ""}

class KeyPointParser(Parser):
    def parse(
        self,
        df: pd.DataFrame,
        ctx: Optional[Dict[str, Any]] = None,
        file_path: Optional[str] = None,
    ) -> pd.DataFrame:
        ctx = ctx or {}
        if not file_path:
            file_path = ctx.get("__file_path__", "")

        date_c = self._col(df, ["Date"])
        desc_c = self._col(df, ["Description"])
        ext_c  = self._col(df, ["Extended Description", "Memo"])  # por si algun export lo trae
        amt_c  = self._col(df, ["Amount"])
        bal_c  = self._col(df, ["Balance"])
        tid_c  = self._col(df, ["Transaction ID", "ID"])
        aid_c  = self._col(df, ["Account ID", "Account", "Account Number"])
        nick_c = self._col(df, ["Account Nickname", "Nickname"])
        check_c   = self._col(df, ["Check Number", "CheckNumber", "Check Num", "Check #"])

        df["_amount_clean"]  = utils.clean_amount_series(df[amt_c]) if amt_c else pd.Series([None] * len(df))
        df["_balance_clean"] = utils.clean_amount_series(df[bal_c]) if bal_c else pd.Series([None] * len(df))

        inferred = _infer_from_filename(file_path) if file_path else {"bank_account": "KP", "bank_cc_num": "", "subentity": ""}
        acct_id_first = df[aid_c].astype(str).str.strip().iloc[0] if aid_c and len(df) else ""
        nick_first = df[nick_c].astype(str).str.strip().str.lower().iloc[0] if nick_c and len(df) else ""
        bank_account = str(
            ctx.get("bank_account")
            or KP_ID_TO_ACCOUNT.get(acct_id_first)
            or KP_NICK_TO_ACCOUNT.get(nick_first)
            or inferred.get("bank_account")
            or "KP"
        )
        bank_cc_num  = str(
            ctx.get("bank_cc_num")
            or inferred.get("bank_cc_num")
            or _last4_from(acct_id_first)
            or ""
        )
        subentity    = str(ctx.get("subentity") or inferred.get("subentity") or "")

        if tid_c:
            df["_txn_id"] = df[tid_c].astype(str)
        else:
            df["_txn_id"] = utils.stable_txn_id(
                bank_account=bank_account,
                bank_cc_num=bank_cc_num,
                date_val=df[date_c] if date_c else pd.Series([None] * len(df)),
                amount_val=df["_amount_clean"],
                description_val=df[desc_c] if desc_c else (df[ext_c] if ext_c else pd.Series([None]*len(df))),
                row_index=df.index.to_series(),
            )

        rows = []
        for _, r in df.iterrows():
            # Description is the normal description/memo text
            description = str(r.get(desc_c, "") or "") if desc_c else ""

            # Rule requested: Check Number -> extended_description
            ext_desc = None
            if check_c:
                val = str(r.get(check_c, "") or "").strip()
                ext_desc = val if val else None

            rows.append(
                StandardizedRecord(
                    bank_source="KP",
                    bank_account=bank_account,
                    subentity=subentity,
                    bank_cc_num=bank_cc_num or _last4_from(r.get(aid_c, "")),
                    txn_id=r["_txn_id"],
                    date=pd.to_datetime(r.get(date_c), errors="coerce") if date_c else pd.NaT,
                    amount=r.get("_amount_clean"),
                    description=description,
                    extended_description=ext_desc,
                    balance=r.get("_balance_clean"),
                )
            )
        return self._build_df(rows)
