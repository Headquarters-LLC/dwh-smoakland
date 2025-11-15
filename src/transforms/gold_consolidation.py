# src/transforms/gold_consolidation.py
from __future__ import annotations
import pandas as pd

KEY = ["bank_account", "subentity", "bank_cc_num"]

def compute_prev_balance(df_before: pd.DataFrame) -> pd.DataFrame:
    """
    Input: core.transactions < week_start (puede venir vacío).
    Output: dataframe con KEY + prev_balance (último balance no-nulo por llave).
    """
    if df_before.empty:
        return pd.DataFrame(columns=KEY + ["prev_balance"])
    dfb = (df_before
           .dropna(subset=["balance"])
           .sort_values(KEY + ["date", "txn_id"]))
    prev = (dfb.groupby(KEY, dropna=False)["balance"]
               .last()
               .rename("prev_balance")
               .reset_index())
    return prev

def consolidate_week(df_week_core: pd.DataFrame,
                     prev_balance: pd.DataFrame) -> pd.DataFrame:
    """
    Compute weekly running balance:
    prev_balance + cumsum(amount ORDER BY date, txn_id).
    Returns the gold.bank_consolidation schema subset.
    """
    if df_week_core.empty:
        return pd.DataFrame(columns=[
            *KEY, "date", "txn_id", "week_num",
            "description", "extended_description",
            "amount", "balance", "year"
        ])

    wk = df_week_core.copy()
    if "extended_description" in wk.columns:
        wk["extended_description"] = wk["extended_description"].fillna("")
    else:
        wk["extended_description"] = ""
    wk["amount"] = pd.to_numeric(wk["amount"], errors="coerce").round(4)
    wk["date"]   = pd.to_datetime(wk["date"], errors="coerce").dt.date

    # join prev_balance (si falta, 0)
    wk = wk.merge(prev_balance, on=KEY, how="left")
    wk["prev_balance"] = wk["prev_balance"].fillna(0.0)

    # running sum por llave en orden estable
    wk = wk.sort_values(KEY + ["date", "txn_id"])
    wk["run"] = (wk.groupby(KEY, dropna=False)["amount"]
                   .cumsum().fillna(0.0))
    wk["balance"] = (wk["prev_balance"] + wk["run"]).round(4)

    # enrich year
    wk["year"] = pd.to_datetime(wk["date"]).dt.year

    out = wk[[*KEY, "date", "txn_id", "week_num",
              "description", "extended_description",
              "amount", "balance", "year"]].copy()
    return out

def reconcile_summary(df_week_gold: pd.DataFrame,
                      prev_balance: pd.DataFrame,
                      atol: float = 0.005) -> pd.DataFrame:
    """
    Build a per-key reconciliation summary with a verdict:
      end_prev  = last known balance before week (prev_balance)
      end_curr  = last weekly balance (from gold_week)
      delta_bal = end_curr - end_prev
      sum_amt   = weekly sum(amount)
      diff      = abs(delta_bal - sum_amt)
      verdict   = 'OK' if diff <= atol else 'MISMATCH'
    """
    if df_week_gold.empty:
        return pd.DataFrame(columns=[*KEY, "end_prev", "end_curr", "sum_amt", "delta_bal", "diff", "verdict"])

    end_prev = prev_balance.set_index(KEY)["prev_balance"].rename("end_prev")

    last_bal = (
        df_week_gold
        .sort_values(KEY + ["date"])
        .groupby(KEY, dropna=False)["balance"]
        .last()
        .rename("end_curr")
    )

    sum_amt = (
        df_week_gold
        .groupby(KEY, dropna=False)["amount"]
        .sum()
        .rename("sum_amt")
    )

    s = pd.concat([end_prev, last_bal, sum_amt], axis=1).fillna(0.0).reset_index()
    s["delta_bal"] = (s["end_curr"] - s["end_prev"]).round(4)
    s["sum_amt"]   = s["sum_amt"].round(4)
    s["diff"]      = (s["delta_bal"] - s["sum_amt"]).abs().round(4)
    s["verdict"]   = s["diff"].le(atol).map({True: "OK", False: "MISMATCH"})
    return s

def reconcile(df_week_gold: pd.DataFrame,
              prev_balance: pd.DataFrame,
              atol: float = 0.005) -> pd.DataFrame:
    """
    Backward-compatible helper: returns only mismatches.
    (Uses reconcile_summary under the hood.)
    """
    summary = reconcile_summary(df_week_gold, prev_balance, atol=atol)
    return summary[summary["verdict"] == "MISMATCH"].reset_index(drop=True)
