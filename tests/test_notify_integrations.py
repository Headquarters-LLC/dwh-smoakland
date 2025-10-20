# tests/test_notify_integrations.py
import pandas as pd
import pytest
from airflow.models import Variable
from src.notify.recon import notify_recon_failure

def _get(name: str) -> str:
    try:
        return (Variable.get(name) or "").strip()
    except Exception:
        return ""

@pytest.fixture
def fake_ctx(tmp_path):
    report_path = tmp_path / "recon_fail_2025-09-15_2025-09-21.csv"
    pd.DataFrame([{
        "bank_account": "TPH", "subentity": "SaCal", "bank_cc_num": "5597",
        "end_prev": 100.0, "end_curr": 130.0, "sum_amt": 20.0, "delta_bal": 30.0, "diff": 10.0
    }]).to_csv(report_path, index=False)

    class _TI:
        def __init__(self, m): self._m = m
        def xcom_pull(self, task_ids=None, key=None): return self._m.get(key)

    return {"ti": _TI({
        "week_start": "2025-09-15",
        "week_end": "2025-09-21",
        "recon_report_path": str(report_path),
    })}

@pytest.mark.live_notify
def test_live_notify_slack(fake_ctx):
    if not (_get("NOTIFY_SLACK_CONN_ID") or _get("SLACK_WEBHOOK")):
        pytest.skip("Slack no configurado")
    notify_recon_failure(fake_ctx)
    assert True

@pytest.mark.live_notify
def test_live_notify_email(fake_ctx):
    if not (_get("NOTIFY_EMAIL_TO") or _get("NOTIFY_TO")):
        pytest.skip("Email no configurado")
    notify_recon_failure(fake_ctx)
    assert True
