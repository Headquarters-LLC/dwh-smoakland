# src/notify/handlers.py
from __future__ import annotations
from typing import Sequence, Optional
from airflow.models import Variable
from airflow.operators.email import EmailOperator

try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    _HAS_SLACK = True
except Exception:
    SlackWebhookOperator = None  # type: ignore
    _HAS_SLACK = False


def _get_var(name: str, default: Optional[str] = "") -> str:
    """Small helper to read Airflow Variables safely."""
    try:
        return (Variable.get(name, default_var=default) or "").strip()
    except Exception:
        return default or ""


def send_email(subject: str, html: str, files: Optional[Sequence[str]] = None) -> None:
    """
    Send email using Airflow's EmailOperator + SMTP Connection.
    Uses Variable SMTP_CONN_ID (defaults to 'smtp_default').
    Recipients from Variable NOTIFY_EMAIL_TO (o NOTIFY_TO como fallback).
    """
    to = Variable.get("NOTIFY_EMAIL_TO", default_var=Variable.get("NOTIFY_TO", "")).strip()
    if not to:
        return

    conn_id = Variable.get("SMTP_CONN_ID", default_var="smtp_default").strip()
    kwargs = dict(
        task_id="__inline_email_notify",
        to=to,
        subject=subject,
        html_content=html,
        files=list(files or []),
        conn_id=conn_id,
    )
    EmailOperator(**kwargs).execute(context={})


def _try_slack_via_conn(message: str, conn_id: str) -> bool:
    """
    Preferred path: use an Airflow Connection.
    Works with either:
      - Conn Type: Slack Webhook  (param: slack_webhook_conn_id)
      - Conn Type: HTTP (host=hooks.slack.com + Extra.endpoint=services/XXX)
    """
    if not _HAS_SLACK or not conn_id:
        return False

    try:
        # Newer provider param
        SlackWebhookOperator(
            task_id="__inline_slack_notify_conn",
            slack_webhook_conn_id=conn_id,  # type: ignore[arg-type]
            message=message,
        ).execute(context={})
        print(f"[notify] Slack sent via connection '{conn_id}' (slack_webhook_conn_id)")
        return True
    except TypeError:
        # Older provider param
        try:
            SlackWebhookOperator(
                task_id="__inline_slack_notify_http",
                http_conn_id=conn_id,  # type: ignore[arg-type]
                message=message,
            ).execute(context={})
            print(f"[notify] Slack sent via connection '{conn_id}' (http_conn_id)")
            return True
        except Exception as e:
            print(f"[notify] Slack via http_conn_id failed: {e}")
            return False
    except Exception as e:
        print(f"[notify] Slack via connection failed: {e}")
        return False


def send_slack(message: str) -> None:
    """
    Resolution order:
      1) Variable NOTIFY_SLACK_CONN_ID (default 'slack_default') → use Airflow Connection.
      2) Variable SLACK_WEBHOOK → use direct webhook URL.
      3) Skip if neither is set.
    """
    if not _HAS_SLACK:
        print("[notify] Slack provider not installed; skipping.")
        return

    conn_id = _get_var("NOTIFY_SLACK_CONN_ID", "slack_default")
    if conn_id and _try_slack_via_conn(message, conn_id):
        return

    webhook = _get_var("SLACK_WEBHOOK")
    if webhook:
        SlackWebhookOperator(
            task_id="__inline_slack_notify_var",
            http_conn_id=None,
            webhook_token=webhook,  # Full URL works here
            message=message,
        ).execute(context={})
        print("[notify] Slack sent via SLACK_WEBHOOK variable")
        return

    print("[notify] Slack skipped: neither connection nor SLACK_WEBHOOK configured")
