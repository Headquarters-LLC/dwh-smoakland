import pandas as pd

def append_idempotent(df: pd.DataFrame) -> pd.DataFrame:
    # placeholder: ensure deduplication by txn_id
    if df is None or df.empty:
        return df
    return df.drop_duplicates(subset=["txn_id"], keep="last").reset_index(drop=True)
