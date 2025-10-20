# src/warehouse/warehouse_base.py
from __future__ import annotations
import pandas as pd
from abc import ABC, abstractmethod
from typing import List

class WarehouseBase(ABC):
    @abstractmethod
    def upsert_dataframe(self, df: pd.DataFrame, table: str, key_cols: List[str]): ...
    @abstractmethod
    def fetch_core_between(self, start_date, end_date) -> pd.DataFrame: ...
    @abstractmethod
    def fetch_core_before(self, start_date) -> pd.DataFrame: ...
    @abstractmethod
    def upsert_gold_consolidation_week(self, df: pd.DataFrame, week_start, week_end) -> None: ...
    @abstractmethod
    def upsert_gold_categorized_week(self, df: pd.DataFrame, week_start, week_end) -> None: ...
