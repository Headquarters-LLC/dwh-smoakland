import pandas as pd
import numpy as np
from src.transforms.gold_consolidation import (
    compute_prev_balance, consolidate_week, reconcile_summary, reconcile
)

KEY = ["bank_account", "subentity", "bank_cc_num"]

def _mk_prev(df_before_rows):
    df = pd.DataFrame(df_before_rows)
    return compute_prev_balance(df)

def test_reconcile_ok():
    # Prev balance: 100 for TPH|SaCal|5597
    prev = _mk_prev([
        {"bank_account": "TPH", "subentity": "SaCal", "bank_cc_num": "5597",
         "date": "2025-09-15", "txn_id":"a1", "balance": 100.0},
    ])

    # Week with net +20 (30 - 10), ending balance 120
    df_week = pd.DataFrame([
        {"bank_account": "TPH", "subentity": "SaCal", "bank_cc_num": "5597",
         "date": "2025-09-22", "txn_id":"w1", "amount": 30.0, "week_num": 39,
         "description":"x", "extended_description":None},
        {"bank_account": "TPH", "subentity": "SaCal", "bank_cc_num": "5597",
         "date": "2025-09-23", "txn_id":"w2", "amount": -10.0, "week_num": 39,
         "description":"y", "extended_description":None},
    ])

    gold = consolidate_week(df_week, prev)
    summary = reconcile_summary(gold, prev, atol=0.0001)

    row = summary.iloc[0]
    assert row["end_prev"] == 100.0
    assert row["sum_amt"] == 20.0
    assert abs(row["delta_bal"] - 20.0) < 1e-9
    assert row["verdict"] == "OK"

    mismatches = reconcile(gold, prev, atol=0.0001)
    assert mismatches.empty

def test_reconcile_fail():
    # Prev balance: 0
    prev = _mk_prev([
        {"bank_account": "TPH", "subentity": "SaCal", "bank_cc_num": "5597",
         "date": "2025-09-15", "txn_id":"a1", "balance": 0.0},
    ])

    # Week: here we add 50, but forget to set the ending balance
    bad_week = pd.DataFrame([
        {"bank_account": "TPH", "subentity": "SaCal", "bank_cc_num": "5597",
         "date": "2025-09-22", "txn_id":"w1", "amount": 50.0, "week_num": 39,
         "description":"x", "extended_description":None},
    ])

    gold = consolidate_week(bad_week, prev)
    gold.loc[:, "balance"] = 30.0

    summary = reconcile_summary(gold, prev, atol=0.0001)
    row = summary.iloc[0]
    assert row["sum_amt"] == 50.0
    assert row["end_curr"] == 30.0
    assert row["delta_bal"] == 30.0
    assert row["verdict"] == "MISMATCH"

    mismatches = reconcile(gold, prev, atol=0.0001)
    assert not mismatches.empty
