# src/notify/recon.py
from __future__ import annotations
import html
from typing import Dict, Any

from .handlers import send_email, send_slack


def notify_recon_failure(ctx: Dict[str, Any]) -> None:
    """
    Build and send the Person-in-the-Loop notification when reconciliation fails.
    Expects these XComs pushed by 'reconcile_and_write':
      - recon_report_path (str)
      - week_start (YYYY-MM-DD)
      - week_end   (YYYY-MM-DD)
    """
    ti = ctx["ti"]
    week_start = ti.xcom_pull(task_ids="reconcile_and_write", key="week_start") or "N/A"
    week_end   = ti.xcom_pull(task_ids="reconcile_and_write", key="week_end") or "N/A"
    report     = ti.xcom_pull(task_ids="reconcile_and_write", key="recon_report_path") or "N/A"

    # Email
    subject = f"❌ Reconciliation failed: {week_start} → {week_end}"
    html_body = f"""
    <p>La conciliación semanal no cuadró.</p>
    <ul>
      <li><b>Semana:</b> {html.escape(week_start)} → {html.escape(week_end)}</li>
      <li><b>Reporte:</b> {html.escape(report)}</li>
    </ul>
    <p>Revisa el CSV adjunto para ver las combinaciones con diferencia &gt; tolerancia.</p>
    """
    send_email(subject=subject, html=html_body, files=[report])

    slack_msg = (
        f":x: *Recon failed*\n"
        f"Week: {week_start} → {week_end}\n"
        f"Report: {report}"
    )
    send_slack(slack_msg)
