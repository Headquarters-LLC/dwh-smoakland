import logging
import pandas as pd

from src.pipelines.qbo_export import (
    build_expenses,
    build_deposits,
    export_deposits_multi,
    export_expenses_multi,
)
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


class _FakeExporter:
    def __init__(self):
        self.sent_deposits = []
        self.sent_expenses = []

    def send_deposits(self, client_id, deposits, **kwargs):
        self.sent_deposits.append({"client_id": client_id, "txn_ids": [d.txn_id for d in deposits]})
        return [{"client_id": client_id, "count": len(deposits)}]

    def send_expenses(self, client_id, expenses, **kwargs):
        self.sent_expenses.append({"client_id": client_id, "txn_ids": [e.txn_id for e in expenses]})
        return [{"client_id": client_id, "count": len(expenses)}]


def test_export_deposits_multi_routes_by_realme(monkeypatch, caplog):
    from src.pipelines import qbo_export

    df = pd.DataFrame(
        [
            {
                "txn_id": "d1",
                "amount": 100.0,
                "bank_account": "BA",
                "bank_cc_num": "1111",
                "payee_vendor": "Client A",
                "description": "dep1",
                "date": "2025-01-01",
                "qbo_account": "Income",
                "qbo_sub_account": "Sales",
                "realme_client_name": "DeliverMD",
            },
            {
                "txn_id": "d2",
                "amount": 150.0,
                "bank_account": "BA",
                "bank_cc_num": "1111",
                "payee_vendor": "Client A",
                "description": "dep2",
                "date": "2025-01-02",
                "qbo_account": "Income",
                "qbo_sub_account": "Sales",
                "realme_client_name": "DeliverMD",
            },
            {
                "txn_id": "d3",
                "amount": 200.0,
                "bank_account": "BA2",
                "bank_cc_num": "2222",
                "payee_vendor": "Client B",
                "description": "dep3",
                "date": "2025-01-03",
                "qbo_account": "Income",
                "qbo_sub_account": "Sales",
                "realme_client_name": "TPH786",
            },
            {
                "txn_id": "d4",
                "amount": 75.0,
                "bank_account": "BA3",
                "bank_cc_num": "3333",
                "payee_vendor": "Client C",
                "description": "dep4",
                "date": "2025-01-04",
                "qbo_account": "Income",
                "qbo_sub_account": "Sales",
                "realme_client_name": "UnknownRealme",
            },
            {
                "txn_id": "d5",
                "amount": 50.0,
                "bank_account": "BA3",
                "bank_cc_num": "3333",
                "payee_vendor": "Client D",
                "description": "dep5",
                "date": "2025-01-05",
                "qbo_account": "Income",
                "qbo_sub_account": "Sales",
                "realme_client_name": None,
            },
        ]
    )

    monkeypatch.setattr(qbo_export, "load_categorized_transactions", lambda *args, **kwargs: df)
    monkeypatch.setattr(qbo_export, "_write_skipped_report", lambda *args, **kwargs: None)

    fake_exporter = _FakeExporter()
    mappings = {"DeliverMD": "client-1", "TPH786": "client-2"}
    with caplog.at_level(logging.WARNING):
        result = export_deposits_multi(
            week_start="2025-01-01",
            week_end="2025-01-07",
            realme_to_client=mappings,
            exporter=fake_exporter,
            environment="sandbox",
            auto_create=True,
        )

    deposits_by_client = {rec["client_id"]: rec["txn_ids"] for rec in fake_exporter.sent_deposits}
    assert deposits_by_client["client-1"] == ["d1", "d2"]
    assert deposits_by_client["client-2"] == ["d3"]
    assert "UnknownRealme" not in result["per_realme"]
    assert "client-1" in deposits_by_client and "client-2" in deposits_by_client
    assert all("d4" not in txns and "d5" not in txns for txns in deposits_by_client.values())
    assert result["total_realme"] == 2
    assert any("missing realme_client_name" in rec.message for rec in caplog.records)
    assert any("mapping_not_found" in rec.message for rec in caplog.records)


def test_export_expenses_multi_handles_empty_and_missing_mapping(monkeypatch):
    from src.pipelines import qbo_export

    df = pd.DataFrame(
        [
            {
                "txn_id": "e1",
                "amount": -25.0,
                "bank_account": "BA",
                "bank_cc_num": "4444",
                "payee_vendor": "Vendor X",
                "description": "exp1",
                "date": "2025-01-02",
                "qbo_account": "Expenses",
                "qbo_sub_account": "Ops",
                "realme_client_name": "UnknownRealme",
            }
        ]
    )

    monkeypatch.setattr(qbo_export, "load_categorized_transactions", lambda *args, **kwargs: df)
    monkeypatch.setattr(qbo_export, "_write_skipped_report", lambda *args, **kwargs: None)
    fake_exporter = _FakeExporter()

    result = export_expenses_multi(
        week_start="2025-01-01",
        week_end="2025-01-07",
        realme_to_client={},
        exporter=fake_exporter,
        environment="sandbox",
    )
    assert result["per_realme"] == {}
    assert fake_exporter.sent_expenses == []

    monkeypatch.setattr(qbo_export, "load_categorized_transactions", lambda *args, **kwargs: pd.DataFrame())
    empty_result = export_expenses_multi(
        week_start="2025-01-01",
        week_end="2025-01-07",
        realme_to_client={"DeliverMD": "client-1"},
        exporter=fake_exporter,
    )
    assert empty_result["total_realme"] == 0
    assert empty_result["per_realme"] == {}
