#warehouse_duckdb.py

import duckdb
import pandas as pd
from .warehouse_base import WarehouseBase

_RESOLVED_COLS = [
    "payee_vendor",
    "cf_account",
    "dashboard_1",
    "budget_owner",
    "entity_qbo",
    "qbo_account",
    "qbo_sub_account",
]

class DuckDBWarehouse(WarehouseBase):
    def __init__(self, db_path: str = "local.duckdb"):
        self.db_path = db_path
        self.con = duckdb.connect(self.db_path)

    # ---------- internal helpers ----------

    def _sanitize_df_for_duckdb(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Make DataFrame types/nulls DuckDB-friendly:
        - Ensure numeric columns are really numeric (not 'object' with 'nan').
        - Ensure datetimes are proper pandas datetime dtype.
        - Ensure text columns keep NULLs as None (not the literal 'nan').
        """
        out = df.copy()

        # Known numeric columns in core.transactions
        numeric_cols = [c for c in ["amount", "balance", "week_num"] if c in out.columns]
        for c in numeric_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        if "week_num" in out.columns:
            out["week_num"] = out["week_num"].astype("Int64")

        # Any "_confidence" columns should be float
        conf_cols = [c for c in out.columns if c.endswith("_confidence")]
        for c in conf_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")

        # Treat calendar fields as nullable integers (not text)
        if "year" in out.columns:
            out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")

        # Dates (DATE)
        for c in ["date", "week_start", "week_end"]:
            if c in out.columns:
                out[c] = pd.to_datetime(out[c], errors="coerce").dt.date

        # Timestamps
        if "ingest_ts" in out.columns:
            out["ingest_ts"] = pd.to_datetime(out["ingest_ts"], errors="coerce")

        # Text-like columns (excluding year which we set as Int64)
        base_text_cols = [
            "bank_source", "bank_account", "subentity", "bank_cc_num",
            "txn_id", "description", "extended_description", "ingest_batch_id",
        ]
        resolved_value_text_cols = [c for c in _RESOLVED_COLS if c in out.columns]
        lineage_text_cols = [c for c in out.columns if c.endswith("_rule_tag") or c.endswith("_source")]
        text_cols = [c for c in (base_text_cols + resolved_value_text_cols + lineage_text_cols) if c in out.columns]

        for c in text_cols:
            out[c] = out[c].astype("string")
            # Optional: keep this if quieres NULL explícito en DuckDB.
            out[c] = out[c].where(~out[c].isna(), None)

        # For any remaining object columns, avoid literal 'nan'
        for c in out.columns:
            if out[c].dtype == "object":
                out[c] = out[c].where(pd.notna(out[c]), None)

        return out

    # ---------- public API ----------

    def upsert_dataframe(self, df: pd.DataFrame, table: str, key_cols: list[str] | None = None):
        """
        Idempotent upsert compatible with DuckDB versions without MERGE:
        - Creates the table if it doesn't exist using the *exact schema* from the DataFrame.
        - If key_cols provided: deletes conflicting rows (by key) and then inserts.
        - Always inserts by *column name* (not by position).

        Also sanitizes dtypes and NULLs to avoid 'nan' strings in numeric columns.
        """
        if df is None or df.empty:
            return

        df = self._sanitize_df_for_duckdb(df)

        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        cols = list(df.columns)
        col_list = ", ".join([f'"{c}"' for c in cols])

        self.con.register("df", df)

        self.con.execute(
            f'CREATE TABLE IF NOT EXISTS {table} AS SELECT {col_list} FROM df WHERE 1=0'
        )

        if key_cols:
            key_cols = [str(k) for k in key_cols]
            on_clause = " AND ".join([f't."{k}" = s."{k}"' for k in key_cols])
            self.con.execute(f'DELETE FROM {table} t USING df s WHERE {on_clause}')

        self.con.execute(f'INSERT INTO {table} ({col_list}) SELECT {col_list} FROM df')

    def fetch_core_between(self, start_date, end_date) -> pd.DataFrame:
        return self.con.execute("""
            SELECT *
            FROM core_transactions
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date]).fetchdf()

    def fetch_core_before(self, start_date) -> pd.DataFrame:
        return self.con.execute("""
            SELECT *
            FROM core_transactions
            WHERE date < ?
        """, [start_date]).fetchdf()

    def upsert_gold_consolidation_week(self, df: pd.DataFrame, week_start, week_end) -> None:
        if df is None or df.empty:
            return

        df = self._sanitize_df_for_duckdb(df)

        self.con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        self.con.register("week_df", df)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS gold.bank_consolidation AS
            SELECT * FROM week_df WHERE 1=0
        """)
        self.con.execute("""
            DELETE FROM gold.bank_consolidation
            WHERE date BETWEEN ? AND ?
        """, [week_start, week_end])
        self.con.execute("""
            INSERT INTO gold.bank_consolidation
            SELECT * FROM week_df
        """)

    def upsert_gold_categorized_week(self, df: pd.DataFrame, week_start, week_end) -> None:
        if df is None or df.empty:
            return

        df = self._sanitize_df_for_duckdb(df)

        self.con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        self.con.register("wdf", df)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS gold.categorized_bank_cc AS
            SELECT * FROM wdf WHERE 1=0
        """)

        self.con.execute("""
            DELETE FROM gold.categorized_bank_cc
            WHERE date BETWEEN ? AND ?
        """, [week_start, week_end])

        self.con.execute("INSERT INTO gold.categorized_bank_cc SELECT * FROM wdf")
