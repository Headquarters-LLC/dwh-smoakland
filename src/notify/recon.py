# src/notify/recon.py
from __future__ import annotations
import html
import os
from typing import Any, Dict

from .handlers import send_email, send_slack


def _pull_from_xcom(airflow_ctx: Dict[str, Any] | None, key: str) -> Any:
    try:
        if airflow_ctx and "ti" in airflow_ctx:
            return airflow_ctx["ti"].xcom_pull(task_ids="reconcile_and_write", key=key)
    except Exception:
        pass
    return None


def notify_recon_failure(
    *,
    week_start: str | None = None,
    week_end: str | None = None,
    ok: bool | None = None,
    paths: Dict[str, str] | None = None,     # {"fail_csv": "...", "summary_csv": "..."}
    airflow_ctx: Dict[str, Any] | None = None,
    week_info: Dict[str, Any] | None = None,
    notify_email: str | None = None,
    **_: Any,  # tolerate future kwargs without breaking
) -> None:
    """
    Sends a notification when the weekly reconciliation fails.

    Accepts explicit data from the DAG (week_start/week_end/paths), and if not provided,
    falls back to XComs from the 'reconcile_and_write' task:
      - week_start (YYYY-MM-DD)
      - week_end   (YYYY-MM-DD)
      - recon_report_path (CSV with differences)
      - recon_summary_path (summary CSV)
    """

    # Fallbacks to provided week_info, then XCom if not passed via kwargs
    if not week_start:
        week_start = (week_info or {}).get("week_start") or _pull_from_xcom(airflow_ctx, "week_start") or "N/A"
    if not week_end:
        week_end = (week_info or {}).get("week_end") or _pull_from_xcom(airflow_ctx, "week_end") or "N/A"

    fail_csv   = (paths or {}).get("fail_csv")    or _pull_from_xcom(airflow_ctx, "recon_report_path")  or "N/A"
    summary_csv= (paths or {}).get("summary_csv") or _pull_from_xcom(airflow_ctx, "recon_summary_path")

    # Best-effort: if paths missing but we have week bounds, try well-known locations
    if (not fail_csv or fail_csv == "N/A") and week_start != "N/A" and week_end != "N/A":
        candidate = f"/opt/airflow/logs/reports/recon_fail_{week_start}_{week_end}.csv"
        if os.path.exists(candidate):
            fail_csv = candidate
    if (not summary_csv or summary_csv == "N/A") and week_start != "N/A" and week_end != "N/A":
        candidate = f"/opt/airflow/logs/reports/recon_summary_{week_start}_{week_end}.csv"
        if os.path.exists(candidate):
            summary_csv = candidate

    # Email
    subject = f"[FAIL] Reconciliation failed: {week_start} -> {week_end}"
    html_body = f"""
    <p>The weekly reconciliation did not balance.</p>
    <ul>
      <li><b>Week:</b> {html.escape(str(week_start))} -> {html.escape(str(week_end))}</li>
      <li><b>Report:</b> {html.escape(str(fail_csv))}</li>
    </ul>
    <p>Check the attached CSV for combinations with differences greater than the tolerance.</p>
    """
    files = [f for f in [fail_csv, summary_csv] if f and f != "N/A"]
    recipients = [notify_email] if notify_email else None
    send_email(subject=subject, html=html_body, files=files, recipients_override=recipients)

    # Slack
    slack_msg = (
        f":x: *Reconciliation failed*\n"
        f"Week: {week_start} -> {week_end}\n"
        f"Report: {fail_csv}"
        + (f"\nSummary: {summary_csv}" if summary_csv and summary_csv != "N/A" else "")
    )
    send_slack(slack_msg)
