# src/transforms/normalize.py
import pandas as pd
from datetime import datetime


def _clean_amount_series(series: pd.Series) -> pd.Series:
    """
    Clean and convert financial amount strings to float.

    Handles:
      - Currency symbols and thousands separators: $, ,
      - Parentheses for negatives: (123.45) -> -123.45
      - Common suffixes: CR, DR, USD, MXN
      - Whitespaces and quotes
    Returns float dtype with NaN for unparseable values.
    """
    if series is None:
        return pd.Series(dtype="float64")

    s = series.astype(str).str.strip()

    # Convert (123.45) -> -123.45
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)

    # Remove currency/locale noise: $, commas, quotes, spaces
    s = s.str.replace(r"[,$\s\"]", "", regex=True)

    # Drop common suffixes (credit/debit flags or currency units)
    s = s.str.replace(r"(CR|DR|USD|MXN)$", "", regex=True, case=False)

    # If any leftover non-numeric chars remain (rare), keep only sign, digits, and dot
    s = s.str.replace(r"[^0-9\.\-\+]", "", regex=True)

    # Finally to numeric
    return pd.to_numeric(s, errors="coerce")


def normalize_df(df: pd.DataFrame, week_start: datetime, week_end: datetime) -> pd.DataFrame:
    """
    Normalize a raw bank CSV DataFrame into a clean schema:
      [txn_id, date, amount, vendor, week_start, week_end]

    Supports the KP bank export format (columns may include:
      'Account ID', 'Transaction ID', 'Date', 'Description', 'Category',
      'Tags', 'Amount', 'Balance') and degrades gracefully if some columns
    are missing.
    """
    cols = ["txn_id", "date", "amount", "vendor", "week_start", "week_end"]

    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    out = pd.DataFrame()

    # ---- txn_id (prefer composite Account|Transaction for idempotency) ----
    if "Account ID" in df.columns and "Transaction ID" in df.columns:
        out["txn_id"] = (
            df["Account ID"].astype(str).str.strip()
            + "|"
            + df["Transaction ID"].astype(str).str.strip()
        )
    elif "Transaction ID" in df.columns:
        out["txn_id"] = df["Transaction ID"].astype(str).str.strip()
    elif "txn_id" in df.columns:
        out["txn_id"] = df["txn_id"].astype(str).str.strip()
    else:
        # Fallback: positional id (still stable within one file)
        out["txn_id"] = df.index.astype(str)

    # ---- date ----
    date_col = next((c for c in ["Date", "date", "txn_date"] if c in df.columns), None)
    out["date"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT

    # ---- amount ----
    amount_col = next((c for c in ["Amount", "amount", "amt"] if c in df.columns), None)
    if amount_col:
        out["amount"] = _clean_amount_series(df[amount_col])
    else:
        out["amount"] = pd.Series([pd.NA] * len(df), dtype="float64")

    # ---- vendor / description ----
    vendor_col = next((c for c in ["Description", "Vendor", "vendor"] if c in df.columns), None)
    if vendor_col:
        out["vendor"] = df[vendor_col].astype(str).str.strip()
    else:
        out["vendor"] = pd.Series([pd.NA] * len(df), dtype="object")

    # ---- week metadata ----
    out["week_start"] = week_start
    out["week_end"]   = week_end

    return out[cols]
