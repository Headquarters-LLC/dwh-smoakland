import logging
import pandas as pd

from src.pipelines.qbo_export import build_expenses, build_deposits
from src.pipelines.qbo_export import _filter_skipped_for_report  # type: ignore


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
        expenses, skipped = build_expenses(df)

    assert len(expenses) == 1
    assert expenses[0].txn_id == "ok-1"
    assert any("reason=unknown_vendor" in rec.message for rec in caplog.records)
    assert len(skipped) == 1


def test_build_expenses_skips_unknown_subaccount(caplog):
    df = pd.DataFrame(
        [
            {
                "txn_id": "bad-sub",
                "amount": -10.0,
                "qbo_account": "Expenses",
                "qbo_sub_account": "UNKNOWN",
                "payee_vendor": "Vendor",
                "bank_account": "BA Main",
            },
            {
                "txn_id": "ok",
                "amount": -10.0,
                "qbo_account": "Expenses",
                "qbo_sub_account": "Ops",
                "payee_vendor": "Vendor",
                "bank_account": "BA Main",
            },
        ]
    )

    with caplog.at_level(logging.WARNING):
        expenses, skipped = build_expenses(df)

    assert len(expenses) == 1
    assert expenses[0].txn_id == "ok"
    assert any("reason=unknown_account" in rec.message for rec in caplog.records)
    assert len(skipped) == 1


def test_filter_skipped_excludes_amount_reasons():
    df = pd.DataFrame(
        [
            {"txn_id": "d1", "skip_reason": "non_positive_amount"},
            {"txn_id": "d2", "skip_reason": "unknown_account"},
        ]
    )
    filtered = _filter_skipped_for_report(df, {"non_positive_amount"})
    assert len(filtered) == 1
    assert filtered.iloc[0]["txn_id"] == "d2"


def test_samples_mode_builds_models_and_skips_unknown(tmp_path, monkeypatch, caplog):
    from src.pipelines import qbo_export

    monkeypatch.setattr(qbo_export, "SAMPLES_PATH", tmp_path)

    rows = [
        {
            "txn_id": "d1",
            "amount": 100.0,
            "bank_account": "BA",
            "bank_cc_num": "1234",
            "payee_vendor": "Client A",
            "description": "dep",
            "date": "2025-01-01",
            "qbo_account": "Income",
            "qbo_sub_account": "Sales",
        },
        {
            "txn_id": "e1",
            "amount": -50.0,
            "bank_account": "BA",
            "bank_cc_num": "4321",
            "payee_vendor": "Vendor A",
            "description": "exp1",
            "date": "2025-01-02",
            "qbo_account": "Expenses",
            "qbo_sub_account": "Ops",
            "class": "Ops",
        },
        {
            "txn_id": "e2",
            "amount": -20.0,
            "bank_account": "BA",
            "bank_cc_num": "4321",
            "payee_vendor": "unknown",
            "description": "exp2",
            "date": "2025-01-03",
            "qbo_account": "Expenses",
            "qbo_sub_account": "Ops",
        },
        {
            "txn_id": "e3",
            "amount": -30.0,
            "bank_account": "BA",
            "bank_cc_num": "4321",
            "payee_vendor": "Vendor B",
            "description": "exp3",
            "date": "2025-01-04",
            "qbo_account": "Expenses",
            "qbo_sub_account": "UNKNOWN",
        },
    ]
    df_samples = pd.DataFrame(rows)
    tmp_path.joinpath("samples.csv").write_text(df_samples.to_csv(index=False))

    with caplog.at_level(logging.WARNING):
        df = qbo_export.load_categorized_transactions("2025-01-01", "2025-01-07", source="samples")
        deposits, skipped_deposits = qbo_export.build_deposits(df)
        expenses, skipped_expenses = qbo_export.build_expenses(df)

    assert len(deposits) == 1
    assert deposits[0].deposit_to_account == "BA 1234"
    assert deposits[0].lines[0].entity_type == "Customer"
    assert skipped_deposits.empty

    assert len(expenses) == 1
    assert expenses[0].vendor == "Vendor A"
    assert expenses[0].lines[0].class_name == "Ops"
    assert any("reason=unknown_vendor" in rec.message for rec in caplog.records)
    assert any("reason=unknown_account" in rec.message for rec in caplog.records)
    assert len(skipped_expenses) == 2


def test_load_categorized_transactions_defaults_to_warehouse(monkeypatch):
    from src.pipelines import qbo_export

    class _FakeWarehouse:
        def __init__(self):
            self.called = False

        def fetch_categorized_between(self, start, end):
            self.called = True
            return pd.DataFrame(
                [
                    {
                        "txn_id": "wd-1",
                        "amount": 10.0,
                        "bank_account": "BA",
                        "bank_cc_num": "1234",
                        "payee_vendor": "Client A",
                        "description": "dep",
                        "date": "2025-01-01",
                        "qbo_account": "Income",
                        "qbo_sub_account": "Sales",
                    }
                ]
            )

    fake_wh = _FakeWarehouse()
    monkeypatch.setattr(qbo_export, "_get_warehouse", lambda db_path=None: fake_wh)

    df = qbo_export.load_categorized_transactions("2025-01-01", "2025-01-07", source="invalid")
    assert fake_wh.called is True
    deposits, skipped = qbo_export.build_deposits(df)
    assert len(deposits) == 1
    assert skipped.empty
