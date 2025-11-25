# dags/dag_part2_qbo_export.py
from __future__ import annotations
from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from src.transforms.week_detect import detect_week_bounds, week_bounds_from_weeknum, try_week_from_path
from src.pipelines.qbo_export import export_deposits, export_expenses
from src.integrations.qbo_gateway import config as qbo_config


def _is_truthy(val: str | None) -> bool:
    if val is None:
        return False
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


DEFAULT_ARGS = {"owner": "smoakland", "retries": 0}

with DAG(
    dag_id="part2_qbo_export",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["smoakland", "part2", "qbo"],
) as dag:

    @task()
    def resolve_week(logical_date: str) -> dict:
        """
        Same resolution strategy as Phase 1:
          1) Airflow Variables WEEK_YEAR + WEEK_NUM
          2) Folder name hint (if INPUT_FOLDER matches weekNN)
          3) logical_date - 7d fallback
        """
        input_folder = Variable.get(
            "INPUT_FOLDER",
            default_var=os.getenv("AIRFLOW_VAR_INPUT_FOLDER", "./data_samples/inbox"),
        )
        week_num = Variable.get("WEEK_NUM", default_var=None)
        week_year = Variable.get("WEEK_YEAR", default_var=None)
        if week_num:
            y = int(week_year) if week_year else pd.to_datetime(logical_date).year
            start, end = week_bounds_from_weeknum(y, int(week_num))
            return {"week_start": start.isoformat(), "week_end": end.isoformat(), "source": "variables"}

        default_dt = pd.to_datetime(logical_date).to_pydatetime()
        wb = try_week_from_path(input_folder, default_dt)
        if wb:
            start, end = wb
            return {"week_start": start.isoformat(), "week_end": end.isoformat(), "source": "folder_name"}

        prior = pd.to_datetime(logical_date) - pd.Timedelta(days=7)
        start, end = detect_week_bounds(prior.to_pydatetime())
        return {"week_start": start.isoformat(), "week_end": end.isoformat(), "source": "logical_date_minus_7"}

    def _common_settings() -> tuple[str, str, bool, str]:
        client_id = Variable.get("QBO_CLIENT_ID", default_var=os.getenv("AIRFLOW_VAR_QBO_CLIENT_ID", ""))
        if not client_id:
            raise ValueError("QBO_CLIENT_ID is required for Phase 2 exports")
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
        source = Variable.get(
            "QBO_EXPORT_SOURCE",
            default_var=os.getenv("AIRFLOW_VAR_QBO_EXPORT_SOURCE", "warehouse"),
        )
        if source.lower() not in {"warehouse", "samples"}:
            source = "warehouse"
        return client_id, env, auto_create, source

    @task()
    def export_deposits_task(week_info: dict) -> dict:
        client_id, env, auto_create, source = _common_settings()
        return export_deposits(
            week_start=week_info["week_start"],
            week_end=week_info["week_end"],
            client_id=client_id,
            environment=env,
            auto_create=auto_create,
            source=source,
        )

    @task()
    def export_expenses_task(week_info: dict) -> dict:
        client_id, env, auto_create, source = _common_settings()
        return export_expenses(
            week_start=week_info["week_start"],
            week_end=week_info["week_end"],
            client_id=client_id,
            environment=env,
            auto_create=auto_create,
            source=source,
        )

    week_info = resolve_week(logical_date="{{ ds }}")
    d = export_deposits_task(week_info)
    e = export_expenses_task(week_info)
    week_info >> [d, e]
