import pandas as pd
from datetime import datetime
from src.transforms.week_detect import detect_week_bounds
from src.transforms.normalize import normalize_df

def test_week_detect():
    start, end = detect_week_bounds(datetime(2025, 1, 8))  # Wednesday
    assert start.weekday() == 0  # Monday
    assert (end - start).days == 6

def test_normalize_df_minimal():
    df = pd.DataFrame({"date":["2025-01-01"], "amount":["10.5"], "vendor":["ABC"]})
    start, end = detect_week_bounds(datetime(2025, 1, 8))
    out = normalize_df(df, start, end)
    assert "week_start" in out.columns
    assert "txn_id" in out.columns
