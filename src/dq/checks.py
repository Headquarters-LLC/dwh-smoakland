import pandas as pd

CRITICAL_COLS = ["txn_id","date","amount","bank_account","bank_cc_num"]

def basic_metrics(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "null_amount": int(df["amount"].isna().sum()) if "amount" in df.columns else None,
        "null_date": int(df["date"].isna().sum()) if "date" in df.columns else None,
        "distinct_txn_id": int(df["txn_id"].nunique()) if "txn_id" in df.columns else None,
    }

def assert_no_dup_txn_id(df: pd.DataFrame) -> None:
    if "txn_id" in df.columns:
        dup = df["txn_id"].duplicated().sum()
        if dup > 0:
            raise ValueError(f"Duplicate txn_id detected: {dup}")

def assert_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in CRITICAL_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
