# src/notify/handlers.py
from __future__ import annotations
from typing import Sequence, Optional
from airflow.models import Variable
from airflow.utils.email import send_email as airflow_send_email  # <-- correct import
import logging

log = logging.getLogger(__name__)  # avoid basicConfig; Airflow already configures logging

try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    _HAS_SLACK = True
except Exception:
    SlackWebhookOperator = None  # type: ignore
    _HAS_SLACK = False


def _get_var(name: str, default: Optional[str] = "") -> str:
    try:
        return (Variable.get(name, default_var=default) or "").strip()
    except Exception:
        return default or ""


def _parse_recipients(raw: str) -> list[str]:
    parts = [p.strip() for p in raw.replace(";", ",").split(",")]
    return [p for p in parts if p]


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
    except Exception as e:
        # Do not break the task if email fails (useful for dev)
        log.exception("Email send failed via conn_id=%s: %s", conn_id, e)


def _try_slack_via_conn(message: str, conn_id: str) -> bool:
    """
    Use an Airflow Connection:
      - Slack Webhook (slack_webhook_conn_id)
      - HTTP (host=hooks.slack.com + Extra.endpoint=services/XXX)
    """
    if not _HAS_SLACK or not conn_id:
        return False

    try:
        # New provider
        SlackWebhookOperator(
            task_id="__inline_slack_notify_conn",
            slack_webhook_conn_id=conn_id,  # type: ignore[arg-type]
            message=message,
        ).execute(context={})
        log.info("[notify] Slack sent via connection '%s' (slack_webhook_conn_id)", conn_id)
        return True
    except TypeError:
        # Older provider (HTTP connection)
        try:
            SlackWebhookOperator(
                task_id="__inline_slack_notify_http",
                http_conn_id=conn_id,  # type: ignore[arg-type]
                message=message,
            ).execute(context={})
            log.info("[notify] Slack sent via connection '%s' (http_conn_id)", conn_id)
            return True
        except Exception as e:
            log.exception("[notify] Slack via http_conn_id failed: %s", e)
            return False
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
                SlackWebhookOperator(
                    task_id="__inline_slack_notify_var",
                    http_conn_id=None,
                    webhook_token=webhook,  # accepts token or full URL depending on provider
                    message=message,
                ).execute(context={})
                log.info("[notify] Slack sent via SLACK_WEBHOOK variable")
            except Exception as e:
                log.exception("[notify] Slack via SLACK_WEBHOOK failed: %s", e)
            return

        log.info("[notify] Slack skipped: neither connection nor SLACK_WEBHOOK configured")
    except Exception as e:
        # Catch-all to ensure the task never fails because of Slack
        log.exception("[notify] Slack send unexpected error: %s", e)
