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
    ]
    df_samples = pd.DataFrame(rows)
    tmp_path.joinpath("samples.csv").write_text(df_samples.to_csv(index=False))

    with caplog.at_level(logging.WARNING):
        df = qbo_export.load_categorized_transactions("2025-01-01", "2025-01-07", source="samples")
        deposits = qbo_export.build_deposits(df)
        expenses = qbo_export.build_expenses(df)

    assert len(deposits) == 1
    assert deposits[0].deposit_to_account == "BA 1234"
    assert deposits[0].lines[0].entity_type == "Customer"

    assert len(expenses) == 1
    assert expenses[0].vendor == "Vendor A"
    assert expenses[0].lines[0].class_name == "Ops"
    assert any("reason=unknown_vendor" in rec.message for rec in caplog.records)


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
    deposits = qbo_export.build_deposits(df)
    assert len(deposits) == 1
