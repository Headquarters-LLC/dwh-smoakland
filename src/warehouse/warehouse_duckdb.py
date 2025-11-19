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
            # Optional: keep this if quieres NULL explicito en DuckDB.
            out[c] = out[c].where(~out[c].isna(), None)
        if "extended_description" in out.columns:
            # Force empty strings over NULL to avoid 'nan' in exports
            out["extended_description"] = out["extended_description"].fillna("").astype("string")

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
        # Always ensure the table exists. Even if df is empty, create it using
        # the DataFrame schema so downstream readers don't fail.
        if df is None:
            return

        df = self._sanitize_df_for_duckdb(df)

        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        cols = list(df.columns)
        if not cols:
            # Nothing we can do without columns
            return
        col_list = ", ".join([f'"{c}"' for c in cols])

        self.con.register("df", df)

        # Create the table (once) with the proper schema
        self.con.execute(
            f'CREATE TABLE IF NOT EXISTS {table} AS SELECT {col_list} FROM df WHERE 1=0'
        )

        # Normalize critical column types at the DB level (idempotent),
        # in case an empty DataFrame led DuckDB to default types incorrectly.
        # Ignore errors if columns are missing.
        for col, dtype in [
            ("date", "DATE"),
            ("week_start", "DATE"),
            ("week_end", "DATE"),
            ("ingest_ts", "TIMESTAMP"),
        ]:
            if col in cols:
                try:
                    self.con.execute(f'ALTER TABLE {table} ALTER COLUMN "{col}" TYPE {dtype} USING CAST("{col}" AS {dtype})')
                except Exception:
                    # Best-effort; continue even if type is already correct or cast fails
                    pass

        # Ensure common text-like columns are VARCHAR to avoid numeric casts
        base_text_cols = [
            "bank_source", "bank_account", "subentity", "bank_cc_num",
            "txn_id", "description", "extended_description", "ingest_batch_id",
        ]
        resolved_value_text_cols = [c for c in _RESOLVED_COLS if c in cols]
        lineage_text_cols = [c for c in cols if c.endswith("_rule_tag") or c.endswith("_source")]
        text_cols = [c for c in (base_text_cols + resolved_value_text_cols + lineage_text_cols) if c in cols]
        for c in text_cols:
            try:
                self.con.execute(f'ALTER TABLE {table} ALTER COLUMN "{c}" TYPE VARCHAR USING CAST("{c}" AS VARCHAR)')
            except Exception:
                pass

        # If there's no data, we're done after ensuring the table exists
        if df.empty:
            return

        if key_cols:
            key_cols = [str(k) for k in key_cols]
            on_clause = " AND ".join([f't."{k}" = s."{k}"' for k in key_cols])
            self.con.execute(f'DELETE FROM {table} t USING df s WHERE {on_clause}')

        self.con.execute(f'INSERT INTO {table} ({col_list}) SELECT {col_list} FROM df')

    def fetch_core_between(self, start_date, end_date) -> pd.DataFrame:
        return self.con.execute("""
            SELECT *
            FROM core_transactions
            WHERE CAST(date AS DATE) BETWEEN ? AND ?
        """, [start_date, end_date]).fetchdf()

    def fetch_core_before(self, start_date) -> pd.DataFrame:
        return self.con.execute("""
            SELECT *
            FROM core_transactions
            WHERE CAST(date AS DATE) < ?
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

        # Split columns: keep only resolved + rule_tag in base table; move
        # *_source and *_confidence to a separate lineage table.
        all_cols = list(df.columns)
        lineage_cols = [c for c in all_cols if c.endswith("_source") or c.endswith("_confidence")]
        base_cols = [c for c in all_cols if c not in lineage_cols]

        self.con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        self.con.register("wdf", df)

        # ---------- Base table (no *_source/_confidence) ----------
        base_col_list = ", ".join([f'"{c}"' for c in base_cols])
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS gold.categorized_bank_cc AS
            SELECT {base_col_list} FROM wdf WHERE 1=0
            """
        )

        # If existed with old columns, drop *_source/_confidence from base
        try:
            info_df = self.con.execute("PRAGMA table_info('gold.categorized_bank_cc')").fetchdf()
            to_drop = [c for c in info_df["name"].tolist() if c.endswith("_source") or c.endswith("_confidence")]
            for col in to_drop:
                self.con.execute(f'ALTER TABLE gold.categorized_bank_cc DROP COLUMN "{col}"')
        except Exception:
            pass

        # Idempotent upsert for week range
        self.con.execute(
            """
            DELETE FROM gold.categorized_bank_cc
            WHERE CAST(date AS DATE) BETWEEN ? AND ?
            """,
            [week_start, week_end],
        )
        self.con.execute(
            f"INSERT INTO gold.categorized_bank_cc ({base_col_list}) SELECT {base_col_list} FROM wdf"
        )

        # ---------- Lineage table: txn_id + *_source/*_confidence ----------
        if lineage_cols:
            lineage_select_cols = ", ".join([f'"{c}"' for c in lineage_cols])
            self.con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS gold.categorized_bank_cc_lineage AS
                SELECT "txn_id", {lineage_select_cols} FROM wdf WHERE 1=0
                """
            )
            # Delete existing rows for these txn_id (overwrite semantics)
            self.con.execute(
                """
                DELETE FROM gold.categorized_bank_cc_lineage l
                USING (SELECT DISTINCT txn_id FROM wdf) s
                WHERE l.txn_id = s.txn_id
                """
            )
            self.con.execute(
                f"INSERT INTO gold.categorized_bank_cc_lineage (\"txn_id\", {lineage_select_cols}) "
                f"SELECT \"txn_id\", {lineage_select_cols} FROM wdf"
            )

            # ---------- View with full columns (base + lineage) ----------
            lineage_view_cols = ", ".join([f'l."{c}"' for c in lineage_cols])
            view_sql = (
                "CREATE OR REPLACE VIEW gold.v_categorized_bank_cc_full AS "
                "SELECT b.*, " + (lineage_view_cols + " " if lineage_view_cols else "") +
                "FROM gold.categorized_bank_cc b LEFT JOIN gold.categorized_bank_cc_lineage l USING (txn_id)"
            )
            self.con.execute(view_sql)
