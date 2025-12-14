# dags/dag_part2_qbo_export.py
from __future__ import annotations
from datetime import datetime
import os
import pandas as pd
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from src.transforms.week_detect import detect_week_bounds, week_bounds_from_weeknum, try_week_from_path
from src.pipelines.qbo_export import export_deposits, export_expenses, export_deposits_multi, export_expenses_multi
from src.integrations.qbo_gateway import config as qbo_config
from src.notify.handlers import send_email
from src.utils.week_resolution import resolve_week_info, resolve_input_mode, cleanup_ephemeral_folder


def _is_truthy(val: str | None) -> bool:
    if val is None:
        return False
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


DEFAULT_ARGS = {"owner": "smoakland", "retries": 0}
log = logging.getLogger(__name__)

with DAG(
    dag_id="part2_qbo_export",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["part2", "qbo"],
    max_active_runs=1,
) as dag:

    @task()
    def build_run_params(logical_date: str) -> dict:
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = dag_run.conf or {} if dag_run else {}
        input_folder = Variable.get(
            "INPUT_FOLDER",
            default_var=os.getenv("AIRFLOW_VAR_INPUT_FOLDER", "./data_samples/inbox"),
        )
        input_mode = resolve_input_mode(conf, input_folder)
        input_mode.update(
            {
                "conf": conf,
                "logical_date": logical_date,
                "run_id": ctx.get("run_id"),
                "notify_email": conf.get("notify_email"),
                "qbo_source": str(
                    Variable.get(
                        "QBO_EXPORT_SOURCE",
                        default_var=os.getenv("AIRFLOW_VAR_QBO_EXPORT_SOURCE", "warehouse"),
                    )
                ).lower(),
            }
        )
        return input_mode

    @task()
    def resolve_week(run_params: dict) -> dict:
        conf = run_params.get("conf", {})
        input_folder = run_params["input_folder"]
        logical_date = run_params["logical_date"]
        return resolve_week_info(
            logical_date=logical_date,
            input_folder=input_folder,
            dag_run_conf=conf,
            extra={
                "input_folder": input_folder,
                "gateway_mode": run_params.get("gateway_mode"),
                "input_subdir": run_params.get("input_subdir"),
                "notify_email": conf.get("notify_email"),
            },
        )

    def _resolve_realme_mapping() -> dict[str, str]:
        mappings = qbo_config.get_realme_clients_map()
        if mappings:
            log.info("[qbo-export] loaded QBO_REALME_CLIENTS mappings count=%s", len(mappings))
        else:
            log.warning("[qbo-export] QBO_REALME_CLIENTS empty or invalid; falling back to single-client mode")
        return mappings

    def _common_settings(realme_to_client: dict[str, str], source_override: str | None = None) -> tuple[str, str, bool, str]:
        client_id = Variable.get("QBO_CLIENT_ID", default_var=os.getenv("AIRFLOW_VAR_QBO_CLIENT_ID", ""))
        if not realme_to_client and not client_id:
            raise ValueError("QBO_CLIENT_ID is required when QBO_REALME_CLIENTS is not configured")
        env = Variable.get(
            "QBO_ENVIRONMENT",
            default_var=os.getenv("AIRFLOW_VAR_QBO_ENVIRONMENT", qbo_config.get_default_environment()),
        )
        auto_create = _is_truthy(
            Variable.get(
                "QBO_AUTO_CREATE",
                default_var=os.getenv("AIRFLOW_VAR_QBO_AUTO_CREATE", "true"),
            )
        )
        if env and str(env).lower() != "sandbox":
            # auto_create is only meaningful for sandbox flows
            auto_create = False
        source = source_override or Variable.get(
            "QBO_EXPORT_SOURCE",
            default_var=os.getenv("AIRFLOW_VAR_QBO_EXPORT_SOURCE", "warehouse"),
        )
        if source.lower() not in {"warehouse", "samples"}:
            source = "warehouse"
        return client_id, env, auto_create, source

    def _samples_base_for_run(week_info: dict, source: str) -> str | None:
        if source.lower() != "samples":
            return None
        if week_info.get("gateway_mode"):
            return week_info.get("input_folder")
        return None

    @task()
    def export_deposits_task(week_info: dict, run_params: dict) -> dict:
        realme_to_client = _resolve_realme_mapping()
        client_id, env, auto_create, source = _common_settings(realme_to_client, source_override=run_params.get("qbo_source"))
        samples_base = _samples_base_for_run(week_info, source)
        if realme_to_client:
            return export_deposits_multi(
                week_start=week_info["week_start"],
                week_end=week_info["week_end"],
                realme_to_client=realme_to_client,
                environment=env,
                auto_create=auto_create,
                source=source,
                samples_base=samples_base,
            )
        return export_deposits(
            week_start=week_info["week_start"],
            week_end=week_info["week_end"],
            client_id=client_id,
            environment=env,
            auto_create=auto_create,
            source=source,
            samples_base=samples_base,
        )

    @task()
    def export_expenses_task(week_info: dict, run_params: dict) -> dict:
        realme_to_client = _resolve_realme_mapping()
        client_id, env, auto_create, source = _common_settings(realme_to_client, source_override=run_params.get("qbo_source"))
        samples_base = _samples_base_for_run(week_info, source)
        if realme_to_client:
            return export_expenses_multi(
                week_start=week_info["week_start"],
                week_end=week_info["week_end"],
                realme_to_client=realme_to_client,
                environment=env,
                auto_create=auto_create,
                source=source,
                samples_base=samples_base,
            )
        return export_expenses(
            week_start=week_info["week_start"],
            week_end=week_info["week_end"],
            client_id=client_id,
            environment=env,
            auto_create=auto_create,
            source=source,
            samples_base=samples_base,
        )

    @task()
    def notify_skipped(week_info: dict, dep_result: dict, exp_result: dict) -> str:
        dep_path = (dep_result or {}).get("skipped_path")
        exp_path = (exp_result or {}).get("skipped_path")
        attachments = [p for p in [dep_path, exp_path] if p]
        if not attachments:
            return "no skipped rows"

        wk_num = dep_result.get("week_num") or exp_result.get("week_num")
        wk_label = f"Week {wk_num}" if wk_num else f"{week_info['week_start']}..{week_info['week_end']}"
        subject = f"[WARN] Skipped QBO exports {wk_label}"
        html = (
            "<p>Some rows were skipped in Phase 2 because they contained UNKNOWN values or were idempotent replays from the gateway.</p>"
            "<p>See attached CSVs (deposits/expenses) for details.</p>"
        )
        recipients = [week_info.get("notify_email")] if week_info.get("notify_email") else None
        send_email(subject=subject, html=html, files=attachments, recipients_override=recipients)
        return f"sent email with {len(attachments)} attachments"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_samples(run_params: dict) -> str:
        source = (run_params.get("qbo_source") or "").lower()
        if source != "samples" or not run_params.get("gateway_mode"):
            return "skip cleanup"
        removed = cleanup_ephemeral_folder(run_params.get("shared_input_folder"), run_params.get("input_subdir"))
        return removed or "cleanup noop"

    run_params = build_run_params(logical_date="{{ ds }}")
    week_info = resolve_week(run_params)
    d = export_deposits_task(week_info, run_params)
    e = export_expenses_task(week_info, run_params)
    n = notify_skipped(week_info, d, e)
    cleanup = cleanup_samples(run_params)

    week_info >> [d, e] >> n
    [d, e, n] >> cleanup
