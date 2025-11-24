import logging
import pandas as pd

from src.pipelines.qbo_export import build_expenses


def test_build_expenses_skips_unknown_vendor(caplog):
    df = pd.DataFrame(
        [
            {
                "txn_id": "ok-1",
                "amount": -25.0,
                "qbo_account": "Expenses",
                "qbo_sub_account": "Ops",
                "payee_vendor": "Vendor Good",
                "bank_account": "BA Main",
            },
            {
                "txn_id": "bad-1",
                "amount": -15.0,
                "qbo_account": "Expenses",
                "qbo_sub_account": "Ops",
                "payee_vendor": "unknown",
                "bank_account": "BA Main",
            },
        ]
    )

    with caplog.at_level(logging.WARNING):
        expenses = build_expenses(df)

    assert len(expenses) == 1
    assert expenses[0].txn_id == "ok-1"
    assert any("reason=unknown_vendor" in rec.message for rec in caplog.records)
