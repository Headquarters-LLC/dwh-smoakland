import hashlib
import pandas as pd
from pandas import Series
from typing import Any, Optional, Tuple

def clean_amount_series(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    s = series.astype(str).str.strip()
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)          # (123.45) -> -123.45
    s = s.str.replace(r"[,$\s\"]", "", regex=True)                # remove $, commas, spaces, quotes
    s = s.str.replace(r"(CR|DR|USD|MXN)$", "", regex=True, case=False)  # remove suffixes
    s = s.str.replace(r"[^0-9\.\-\+]", "", regex=True)            # keep only digits and signs
    return pd.to_numeric(s, errors="coerce")

def _as_series(x: Any, n: int) -> Series:
    """Convert scalar/None/Series to a Series of length n."""
    if isinstance(x, Series):
        return x
    return pd.Series([x] * n)

def stable_txn_id(
    bank_account: Any,
    bank_cc_num: Any,
    date_val: Any,
    amount_val: Any,
    description_val: Any,
    row_index: Any,
) -> pd.Series:
    """Generate a stable transaction ID per row; accepts scalars or Series."""
    if isinstance(amount_val, Series):
        n = len(amount_val)
    elif isinstance(date_val, Series):
        n = len(date_val)
    else:
        n = len(row_index)

    bank_account   = _as_series(bank_account, n).fillna("").astype(str)
    bank_cc_num    = _as_series(bank_cc_num, n).fillna("").astype(str)
    date_val       = pd.to_datetime(_as_series(date_val, n), errors="coerce").astype(str)
    amount_val     = _as_series(amount_val, n).fillna("").astype(str)
    description_val= _as_series(description_val, n).fillna("").astype(str)
    if row_index is None:
        row_index = range(n)
    row_index      = _as_series(row_index, n).astype(str)

    base = bank_account + "|" + bank_cc_num + "|" + date_val + "|" + amount_val + "|" + description_val + "|" + row_index
    return base.apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest()[:16])

def pick_description(desc: Optional[str], ext: Optional[str]) -> Tuple[str, Optional[str]]:
    """Given a description and extended description, pick the best values."""
    d = (str(desc) if desc is not None else "").strip()
    e = (str(ext)  if ext  is not None else "").strip()
    if d and e:
        return (d, None) if d == e else (d, e)
    if d:
        return (d, None)
    if e:
        return (e, None)
    return (None, None)
