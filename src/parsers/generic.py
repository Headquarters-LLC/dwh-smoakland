
import pandas as pd
from .base import Parser, StandardizedRecord

class GenericParser(Parser):
    def parse(self, file_path: str, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        date_c = self._col(df, ["date", "posting date", "transaction date"])
        desc_c = self._col(df, ["description", "extended description", "memo"])
        amt_c  = self._col(df, ["amount", "debit/credit amount", "debit", "credit"])
        bal_c  = self._col(df, ["balance", "running balance"])
        id_c   = self._col(df, ["transaction id", "id", "reference"])

        for _, r in df.iterrows():
            amt = r.get(amt_c, None) if amt_c else None
            if amt is None:
                debit = r.get(self._col(df, ["debit"]), None)
                credit = r.get(self._col(df, ["credit"]), None)
                if debit is not None:
                    try: amt = -abs(float(debit))
                    except: amt = None
                elif credit is not None:
                    try: amt = abs(float(credit))
                    except: amt = None

            rows.append(StandardizedRecord(
                bank_source="generic",
                account_hint="",
                txn_id=str(r.get(id_c, "")) if id_c else "",
                date=pd.to_datetime(r.get(date_c)) if date_c else pd.NaT,
                amount=pd.to_numeric(amt, errors="coerce"),
                vendor=str(r.get(desc_c, ""))[:256] if desc_c else "",
                description=str(r.get(desc_c, "")) if desc_c else "",
                balance=pd.to_numeric(r.get(bal_c), errors="coerce") if bal_c else None,
            ))
        return self._build_df(rows)
