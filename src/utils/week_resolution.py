from __future__ import annotations

from typing import Any, Mapping
import os
import pandas as pd
from airflow.models import Variable

from src.transforms.week_detect import detect_week_bounds, week_bounds_from_weeknum, try_week_from_path


def _coerce_int(val: Any) -> int | None:
    try:
        return int(val) if val is not None and str(val).strip() != "" else None
    except Exception:
        return None


def resolve_week_info(
    *,
    logical_date: str,
    input_folder: str,
    dag_run_conf: Mapping[str, Any] | None = None,
    extra: dict | None = None,
) -> dict:
    """
    Priority:
      0) dag_run.conf -> week_year + week_num
      1) Airflow Variables WEEK_YEAR + WEEK_NUM
      2) logical_date - 7 days
      3) Folder name hint
    """
    conf = dag_run_conf or {}
    conf_year = _coerce_int(conf.get("week_year"))
    conf_num = _coerce_int(conf.get("week_num"))
    if conf_year and conf_num:
        start, end = week_bounds_from_weeknum(conf_year, conf_num)
        source = "conf"
    else:
        var_year = _coerce_int(Variable.get("WEEK_YEAR", default_var=None))
        var_num = _coerce_int(Variable.get("WEEK_NUM", default_var=None))
        if var_year and var_num:
            start, end = week_bounds_from_weeknum(var_year, var_num)
            source = "variables"
        else:
            prior = pd.to_datetime(logical_date) - pd.Timedelta(days=7)
            start, end = detect_week_bounds(prior.to_pydatetime())
            source = "logical_date_minus_7"
            folder_hint = try_week_from_path(input_folder, prior.to_pydatetime())
            if folder_hint and not (start and end):
                start, end = folder_hint
                source = "folder_name"

    if getattr(start, "tzinfo", None):
        start = start.replace(tzinfo=None)
    if getattr(end, "tzinfo", None):
        end = end.replace(tzinfo=None)

    week_info = {
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "source": source,
        "input_folder": os.path.abspath(input_folder),
    }
    if conf_year:
        week_info["week_year"] = conf_year
    if conf_num:
        week_info["week_num"] = conf_num
    if extra:
        week_info.update(extra)
    return week_info
