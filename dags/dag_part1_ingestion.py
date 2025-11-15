# dags/dag_part1_ingestion.py
from __future__ import annotations
from datetime import datetime
from airflow.operators.python import get_current_context
import os
import uuid
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from src.io.storage_local import LocalStorage
from src.parsers import detect_source, get_parser
from src.dq.checks import basic_metrics, assert_no_dup_txn_id, assert_required_columns
from src.transforms.week_detect import (
    detect_week_bounds,
    week_bounds_from_weeknum,
    try_week_from_path,
)
from src.warehouse.warehouse_duckdb import DuckDBWarehouse
from src.transforms.gold_consolidation import compute_prev_balance, consolidate_week, reconcile_summary
from src.notify.recon import notify_recon_failure
from transforms.resolve_categorization import categorize_week


def _is_truthy(val: str | None) -> bool:
    if val is None:
        return False
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


DEFAULT_ARGS = {"owner": "smoakland", "retries": 0}

with DAG(
    dag_id="part1_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,          # Manual or via API
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["smoakland", "part1"],
) as dag:

    @task()
    def list_csvs() -> list[str]:
        """
        List CSVs from the configured input folder (local dev).
        This remains decoupled: for GDrive you’ll swap the adapter.
        """
        input_folder = Variable.get(
            "INPUT_FOLDER",
            default_var=os.getenv("AIRFLOW_VAR_INPUT_FOLDER", "./data_samples/inbox"),
        )
        storage = LocalStorage(base_path=input_folder)
        files = [f for f in storage.list_files(extension=".csv")]
        if not files:
            raise FileNotFoundError(f"No CSV files found in {input_folder}")
        return files

    @task()
    def resolve_week(input_folder: str, logical_date: str) -> dict:
        """
        Priority:
          1) Airflow Variables: WEEK_YEAR + WEEK_NUM
          2) Folder name: .../week_37, .../week37-2025
          3) Fallback: prior week from (logical_date - 7d)  ← semana vencida
        Returns ISO strings and a 'source' hint.
        """
        # 1) Variables
        week_num = Variable.get("WEEK_NUM", default_var=None)
        week_year = Variable.get("WEEK_YEAR", default_var=None)
        if week_num:
            y = int(week_year) if week_year else pd.to_datetime(logical_date).year
            start, end = week_bounds_from_weeknum(y, int(week_num))
            return {"week_start": start.isoformat(), "week_end": end.isoformat(), "source": "variables"}

        # 2) Folder name
        default_dt = pd.to_datetime(logical_date).to_pydatetime()
        wb = try_week_from_path(input_folder, default_dt)
        if wb:
            start, end = wb
            return {"week_start": start.isoformat(), "week_end": end.isoformat(), "source": "folder_name"}

        # 3) Fallback: prior week from (logical_date - 7d)
        prior = pd.to_datetime(logical_date) - pd.Timedelta(days=7)
        start, end = detect_week_bounds(prior.to_pydatetime())
        return {"week_start": start.isoformat(), "week_end": end.isoformat(), "source": "logical_date_minus_7"}

    @task()
    def parse_one(file_path: str) -> str:
        """
        Detect source → parse → standardize to core schema.
        Writes a parquet in logs/staging and returns its path.
        """
        df_raw = pd.read_csv(file_path)
        kind = detect_source(file_path, df_raw.head(5))
        print(f"[router] {file_path} -> {kind} (rows={len(df_raw)})")
        parser = get_parser(kind)

        # Context the parser can use (all optional).
        ctx = {
            "__file_path__": file_path,               
            "bank_account": os.getenv("BANK_ACCOUNT_DEFAULT", ""), 
            "subentity": os.getenv("SUBENTITY_DEFAULT", ""),      
            "bank_cc_num": os.getenv("BANK_LAST4_DEFAULT", ""),   
            "ingest_batch_id": f"manual-{datetime.utcnow():%Y%m%d}",
        }

        # Correct call signature
        df_norm = parser.parse(file_path=file_path, df=df_raw, ctx=ctx)

        # DQ sanity (per-file)
        assert_required_columns(df_norm)

        out_dir = "/opt/airflow/logs/staging"
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{uuid.uuid4().hex}.parquet")
        df_norm.to_parquet(out_path, index=False)
        print(f"[parse_one] wrote {out_path} with shape={df_norm.shape}")
        return out_path


    @task()
    def combine_enrich_check(parquet_paths: list[str], week_info: dict) -> str:
        frames = [pd.read_parquet(p) for p in parquet_paths]
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        #  DQ Rules
        assert_required_columns(df)
        assert_no_dup_txn_id(df)

        # Week enrichment
        week_start = pd.to_datetime(week_info["week_start"])
        week_end   = pd.to_datetime(week_info["week_end"])
        df["week_start"] = week_start
        df["week_end"]   = week_end
        df["week_num"] = pd.to_datetime(df["date"], errors="coerce").dt.isocalendar().week.astype("Int64")
        for c in ["amount", "balance"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        ctx = get_current_context()
        df["ingest_batch_id"] = ctx["run_id"]
        df["ingest_ts"] = pd.Timestamp(ctx["ts"])
        if "extended_description" in df.columns:
            # Keep presentation-friendly blanks rather than NaN/None
            df["extended_description"] = df["extended_description"].fillna("")
        else:
            df["extended_description"] = ""

        # Filter to the week (in case of dirty data)
        mask = (pd.to_datetime(df["date"], errors="coerce") >= week_start) & \
            (pd.to_datetime(df["date"], errors="coerce") <= week_end)
        df = df.loc[mask].reset_index(drop=True)

        m = basic_metrics(df)
        print(f"Combined metrics: {m} | week_source={week_info.get('source')}")

        out_path = "/opt/airflow/logs/core_transactions.parquet"
        df.to_parquet(out_path, index=False)
        return out_path


    @task()
    def write_core_transactions(parquet_path: str) -> str:
        """
        Persist into DuckDB as 'core_transactions' (dev).
        In prod this points to BigQuery.
        """
        wh = DuckDBWarehouse(db_path=os.getenv("DUCKDB_PATH","/opt/airflow/logs/local.duckdb"))
        df = pd.read_parquet(parquet_path)
        wh.upsert_dataframe(df, "core_transactions", key_cols=["txn_id"])
        return f"rows={len(df)} -> core_transactions"

    # wiring (with task mapping)
    input_folder = Variable.get(
        "INPUT_FOLDER",
        default_var=os.getenv("AIRFLOW_VAR_INPUT_FOLDER", "./data_samples/inbox"),
    )
    # logical_date via macro; ds = YYYY-MM-DD (UTC midnight)
    week_info = resolve_week(input_folder=input_folder, logical_date="{{ ds }}")

    files = list_csvs()
    parsed = parse_one.expand(file_path=files)                 # fan-out per file
    combined = combine_enrich_check(parsed, week_info)         # fan-in + week enrich
    rows = write_core_transactions(combined)

    @task()
    def build_week_gold(week_info: dict) -> str:
        wh = DuckDBWarehouse(db_path=os.getenv("DUCKDB_PATH","/opt/airflow/logs/local.duckdb"))
        week_start = pd.to_datetime(week_info["week_start"]).date()
        week_end   = pd.to_datetime(week_info["week_end"]).date()

        df_week = wh.fetch_core_between(week_start, week_end)
        df_prev_all = wh.fetch_core_before(week_start)

        prev = compute_prev_balance(df_prev_all)
        gold_week = consolidate_week(df_week, prev)

        out_path = "/opt/airflow/logs/staging/gold_week.parquet"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        gold_week.to_parquet(out_path, index=False)
        return out_path

    @task()
    def reconcile_and_write(week_info: dict, week_parquet: str) -> str:
        wh = DuckDBWarehouse(db_path=os.getenv("DUCKDB_PATH","/opt/airflow/logs/local.duckdb"))
        week_start = pd.to_datetime(week_info["week_start"]).date()
        week_end   = pd.to_datetime(week_info["week_end"]).date()
        force_recon_fail = _is_truthy(
            Variable.get(
                "FORCE_RECON_FAIL",
                default_var=os.getenv("AIRFLOW_VAR_FORCE_RECON_FAIL", "false"),
            )
        )

        gold_week = pd.read_parquet(week_parquet)
        df_prev_all = wh.fetch_core_before(week_start)
        prev = compute_prev_balance(df_prev_all)

        # Build reconciliation summary (verdict per key)
        summary = reconcile_summary(gold_week, prev, atol=0.005)

        # Always persist the summary as an artifact
        report_dir = "/opt/airflow/logs/reports"
        os.makedirs(report_dir, exist_ok=True)
        summary_path = os.path.join(report_dir, f"recon_summary_{week_start}_{week_end}.csv")
        summary.to_csv(summary_path, index=False)
        print(f"[reconcile] Summary written to {summary_path}")

        # Fail-fast if there are mismatches
        bad = summary[summary["verdict"] == "MISMATCH"]
        if force_recon_fail:
            print("[reconcile] FORCE_RECON_FAIL enabled -> forcing mismatch path for notification test")
            if summary.empty:
                forced_row = {c: None for c in summary.columns}
                forced_row.update({"verdict": "FORCED_FAIL"})
                summary = pd.concat([summary, pd.DataFrame([forced_row])], ignore_index=True)
            else:
                summary = summary.copy()
                summary["verdict"] = "FORCED_FAIL"
            summary.to_csv(summary_path, index=False)
            bad = summary
        if not bad.empty:
            fail_path = os.path.join(report_dir, f"recon_fail_{week_start}_{week_end}.csv")
            bad.to_csv(fail_path, index=False)

            # Push XComs for the notification task
            ti = get_current_context()["ti"]
            ti.xcom_push(key="week_start", value=str(week_start))
            ti.xcom_push(key="week_end", value=str(week_end))
            ti.xcom_push(key="recon_report_path", value=fail_path)
            ti.xcom_push(key="recon_summary_path", value=summary_path)

            # Notify and fail
            raise RuntimeError(f"Reconciliation failed. See {fail_path}")

        # Persist the gold if all OK
        wh.upsert_gold_consolidation_week(gold_week, week_start, week_end)

        # Push XComs for reference
        ti = get_current_context()["ti"]
        ti.xcom_push(key="week_start", value=str(week_start))
        ti.xcom_push(key="week_end", value=str(week_end))
        ti.xcom_push(key="recon_summary_path", value=summary_path)

        return f"gold.bank_consolidation upserted for {week_start}..{week_end}"
    
    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_recon(week_info: dict):
        ti = get_current_context()["ti"]
        ctx = get_current_context()

        week_start = ti.xcom_pull(task_ids="reconcile_and_write", key="week_start")
        week_end   = ti.xcom_pull(task_ids="reconcile_and_write", key="week_end")
        fail_path  = ti.xcom_pull(task_ids="reconcile_and_write", key="recon_report_path")
        summary    = ti.xcom_pull(task_ids="reconcile_and_write", key="recon_summary_path")

        notify_recon_failure(
            week_start=week_start,
            week_end=week_end,
            ok=False,
            paths={"fail_csv": fail_path, "summary_csv": summary},
            airflow_ctx=ctx,
            week_info=week_info,
        )

    @task()
    def categorize_and_write(week_info: dict, week_parquet: str) -> str:
        """
        Categorizes weekly consolidated transactions using all rulebooks
        and writes results to DuckDB (gold.categorized_bank_cc).
        Also generates per-field reports for unknown/unmatched entries.
        """
        wh = DuckDBWarehouse(db_path=os.getenv("DUCKDB_PATH", "/opt/airflow/logs/local.duckdb"))

        week_start = pd.to_datetime(week_info["week_start"]).date()
        week_end   = pd.to_datetime(week_info["week_end"]).date()

        # Load weekly consolidated data from parquet
        gold_week = pd.read_parquet(week_parquet)

        # Apply universal categorization engine (resolves all rulebooks)
        categorized = categorize_week(gold_week)

        # ---------- Generate UNKNOWN reports per resolved field ----------
        report_dir = "/opt/airflow/logs/reports"
        os.makedirs(report_dir, exist_ok=True)

        resolved_cols = [
            "payee_vendor",
            "cf_account",
            "dashboard_1",
            "budget_owner",
            "entity_qbo",
            "qbo_account",
            "qbo_sub_account",
        ]

        for col in resolved_cols:
            src_col = f"{col}_source"
            if src_col not in categorized.columns:
                continue

            unk = categorized[categorized[src_col] == "unknown"]
            if unk.empty:
                continue

            # Group by description fields to identify new rule candidates
            # Include payee_vendor when available — it often improves rule context
            group_cols = [c for c in ["payee_vendor", "description", "extended_description"] if c in unk.columns]

            out_df = (
                unk.groupby(group_cols, dropna=False)
                .agg(cnt=("amount", "size"), sum_amt=("amount", "sum"))
                .reset_index()
                .sort_values(["cnt", "sum_amt"], ascending=[False, False])
            )

            unk_path = os.path.join(report_dir, f"{col}_unknowns_{week_start}_{week_end}.csv")
            out_df.to_csv(unk_path, index=False)

        # ---------- Upsert into gold layer (categorized_bank_cc) ----------
        wh.upsert_gold_categorized_week(categorized, week_start, week_end)

        return f"gold.categorized_bank_cc upserted for {week_start}..{week_end}"


    # wiring:
    week_table = build_week_gold(week_info)
    result = reconcile_and_write(week_info, week_table)
    categorized = categorize_and_write(week_info, week_table)
    rows >> week_table >> result >> categorized
    result >> notify_recon(week_info)
