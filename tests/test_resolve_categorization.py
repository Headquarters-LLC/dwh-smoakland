import pandas as pd

import src.transforms.resolve_categorization as rc


def _base_frame(rows):
    return pd.DataFrame(rows)


def test_bank_account_cc_mapping(monkeypatch):
    monkeypatch.setattr(rc, "RESOLVERS", [])

    df = _base_frame([
        {"bank_account": "DMD", "bank_cc_num": "2035", "date": "2024-01-01", "txn_id": "t1", "week_num": 1, "amount": 0},
        {"bank_account": "ABC", "bank_cc_num": "9999", "date": "2024-01-02", "txn_id": "t2", "week_num": 1, "amount": 0},
    ])

    out = rc.categorize_week(df)
    assert out.loc[0, "bank_account_cc"] == "NBCU 2035"
    assert out.loc[1, "bank_account_cc"] == "ABC 9999"


def test_realme_client_name_derivation(monkeypatch):
    monkeypatch.setattr(rc, "RESOLVERS", [])

    entities = ["SWD", "SSC", "HAH 7 CA", "NY", "SoCal", "TPH", "OTHER", pd.NA]
    df = _base_frame([
        {
            "bank_account": "",
            "bank_cc_num": "",
            "entity_qbo": e,
            "date": "2024-01-01",
            "txn_id": f"e{i}",
            "week_num": 1,
            "amount": 0,
        }
        for i, e in enumerate(entities)
    ])

    out = rc.categorize_week(df)
    expected = [
        "DeliverMD",
        "Sublime Machining",
        "HAH 7 LLC",
        "TPH786",
        "TPH786",
        "TPH786",
        pd.NA,
        pd.NA,
    ]
    for idx, exp in enumerate(expected):
        got = out.loc[idx, "realme_client_name"]
        if exp is pd.NA:
            assert pd.isna(got)
        else:
            assert got == exp


def test_description_cleanup(monkeypatch):
    monkeypatch.setattr(rc, "RESOLVERS", [])

    df = _base_frame([
        {"description": " CHEFSTORE   7567   OAKLAND  CA -71018 ", "date": "2024-01-01", "txn_id": "d1", "week_num": 1, "amount": 0},
        {"description": "Tabbed\t\ttext   here", "date": "2024-01-01", "txn_id": "d2", "week_num": 1, "amount": 0},
        {"description": "multiple   spaces  inside", "date": "2024-01-01", "txn_id": "d3", "week_num": 1, "amount": 0},
        {"description": None, "date": "2024-01-01", "txn_id": "d4", "week_num": 1, "amount": 0},
    ])

    out = rc.categorize_week(df)
    assert out.loc[0, "description"] == "CHEFSTORE 7567 OAKLAND CA -71018"
    assert out.loc[1, "description"] == "Tabbed text here"
    assert out.loc[2, "description"] == "multiple spaces inside"
    assert pd.isna(out.loc[3, "description"]) or out.loc[3, "description"] == ""
