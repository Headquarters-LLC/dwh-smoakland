# src/notify/handlers.py
from __future__ import annotations
from typing import Sequence, Optional
from airflow.models import Variable
from airflow.utils.email import send_email as airflow_send_email  # <-- correct import
from airflow.hooks.base import BaseHook
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import json
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)  # avoid basicConfig; Airflow already configures logging

try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    _HAS_SLACK = True
except Exception:
    SlackWebhookOperator = None  # type: ignore
    SlackWebhookHook = None  # type: ignore
    _HAS_SLACK = False


def _get_var(name: str, default: Optional[str] = "") -> str:
    try:
        return (Variable.get(name, default_var=default) or "").strip()
    except Exception:
        return default or ""


def _parse_recipients(raw: str) -> list[str]:
    parts = [p.strip() for p in raw.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _send_email_via_conn(conn_id: str, recipients: list[str], subject: str, html: str, files: Optional[Sequence[str]]) -> bool:
    """
    Fallback SMTP sender that respects Airflow Connections (handles conn_type='smtps').
    """
    if not conn_id:
        return False

    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as exc:
        log.exception("[notify] SMTP fallback skipped: unable to load connection '%s': %s", conn_id, exc)
        return False

    host = conn.host or "localhost"
    port = conn.port or (465 if conn.conn_type == "smtps" else 25)
    extra = conn.extra_dejson or {}
    username = conn.login or extra.get("username")
    password = conn.password or extra.get("password")
    from_email = extra.get("from_email") or username or "no-reply@example.com"

    def _is_true(val: Optional[str]) -> bool:
        return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

    use_ssl = conn.conn_type == "smtps" or _is_true(extra.get("smtp_ssl")) or port == 465
    use_starttls = _is_true(extra.get("smtp_starttls"))
    timeout = int(extra.get("timeout", 30))

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    for path in files or []:
        try:
            with open(path, "rb") as fh:
                attachment = MIMEApplication(fh.read(), Name=os.path.basename(path))
            attachment["Content-Disposition"] = f'attachment; filename="{os.path.basename(path)}"'
            msg.attach(attachment)
        except FileNotFoundError:
            log.warning("[notify] Attachment missing, skipping: %s", path)

    smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    try:
        with smtp_cls(host=host, port=port, timeout=timeout) as smtp:
            if not use_ssl and use_starttls:
                smtp.starttls()
            if username:
                smtp.login(username, password or "")
            smtp.sendmail(from_email, recipients, msg.as_string())
        log.info("[notify] Email sent via direct SMTP (%s:%s, ssl=%s)", host, port, use_ssl)
        return True
    except Exception as exc:
        log.exception("[notify] SMTP fallback failed via '%s': %s", conn_id, exc)
        return False


def send_email(subject: str, html: str, files: Optional[Sequence[str]] = None) -> None:
    """
    Send an email from a @task Python function without creating an Operator.
    Uses conn_id from Variable SMTP_CONN_ID (default 'smtp_default')
    and recipients from NOTIFY_EMAIL_TO (or NOTIFY_TO).
    """
    to_raw = _get_var("NOTIFY_EMAIL_TO", _get_var("NOTIFY_TO", ""))
    recipients = _parse_recipients(to_raw)
    if not recipients:
        log.info("[notify] Email skipped: NOTIFY_EMAIL_TO is empty")
        return

    conn_id = _get_var("SMTP_CONN_ID", "smtp_default")
    try:
        airflow_send_email(
            to=recipients,
            subject=subject,
            html_content=html,
            files=list(files or []),
            conn_id=conn_id,
        )
        log.info("[notify] Email sent via conn_id='%s' to=%s", conn_id, recipients)
        return
    except Exception as e:
        log.exception("Email send failed via conn_id=%s: %s", conn_id, e)

    # Fallback (handles conn_type='smtps' which Airflow may treat as plain SMTP)
    if not _send_email_via_conn(conn_id, recipients, subject, html, files):
        # Do not break the task if email fails (useful for dev)
        return


def _try_slack_via_conn(message: str, conn_id: str) -> bool:
    """
    Use an Airflow Connection:
      - Slack Webhook (slack_webhook_conn_id)
      - HTTP (host=hooks.slack.com + Extra.endpoint=services/XXX)
    """
    if not _HAS_SLACK or not conn_id:
        return False

    try:
        SlackWebhookOperator(
            task_id="__inline_slack_notify_conn",
            slack_webhook_conn_id=conn_id,  # type: ignore[arg-type]
            message=message,
        ).execute(context={})
        log.info("[notify] Slack sent via connection '%s'", conn_id)
        return True
    except Exception as e:
        log.exception("[notify] Slack via connection failed: %s", e)
        return False


def send_slack(message: str) -> None:
    """
    Resolution order:
      1) Variable NOTIFY_SLACK_CONN_ID (default 'slack_default') → Connection.
      2) Variable SLACK_WEBHOOK → Direct webhook (token or full URL).
      3) If none, skip.
    """
    if not _HAS_SLACK:
        log.info("[notify] Slack provider not installed; skipping.")
        return

    try:
        conn_id = _get_var("NOTIFY_SLACK_CONN_ID", "slack_default")
        if conn_id and _try_slack_via_conn(message, conn_id):
            return

        webhook = _get_var("SLACK_WEBHOOK")
        if webhook:
            try:
                payload = json.dumps({"text": message}).encode("utf-8")
                req = Request(webhook, data=payload, headers={"Content-Type": "application/json"})
                with urlopen(req, timeout=10) as resp:  # nosec - trusted URL provided via Airflow
                    resp.read()
                log.info("[notify] Slack sent via SLACK_WEBHOOK variable (direct POST)")
            except Exception as e:
                log.exception("[notify] Slack via SLACK_WEBHOOK failed: %s", e)
            return

        log.info("[notify] Slack skipped: neither connection nor SLACK_WEBHOOK configured")
    except Exception as e:
        # Catch-all to ensure the task never fails because of Slack
        log.exception("[notify] Slack send unexpected error: %s", e)
