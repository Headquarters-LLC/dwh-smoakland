# src/io/storage_local.py
from __future__ import annotations
import os
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from .storage_base import StorageBase


class LocalStorage(StorageBase):
    """
    Local filesystem implementation.
    """

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def list_files(self, extension: str = "") -> List[str]:
        """
        Recursively walk through base_path and return file paths.
        - extension: '.csv', '.parquet', etc. (case-insensitive).
          If empty string, return all files.
        """
        if not self.base_path.exists():
            return []

        ext = extension.lower().strip()
        out: List[str] = []
        for root, _, fns in os.walk(self.base_path):
            for fn in fns:
                if not ext or fn.lower().endswith(ext):
                    out.append(str(Path(root) / fn))
        out.sort()
        return out

    def read_file(self, path: str) -> pd.DataFrame:
        """
        Read a CSV at 'path' and return a DataFrame (all columns as str).
        This avoids issues like losing leading zeros or long IDs.
        """
        return pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
            engine="python",  # tolerant to messy delimiters/quotes
        )


# ---------- High-level helpers for DAGs ----------

def list_csv_paths(base_dir: str) -> List[Path]:
    """
    Return Path objects for all .csv files in base_dir (non-recursive).
    """
    p = Path(base_dir)
    if not p.exists():
        return []
    return sorted(p.glob("*.csv"))


def read_csv_payloads(base_dir: str) -> List[Dict[str, Any]]:
    """
    Read all CSV files in base_dir and return a list of payload dicts:
    {
        "df": DataFrame,
        "source_file": filename,
        "source_path": full path
    }
    These metadata fields help later when choosing the right parser.
    """
    payloads: List[Dict[str, Any]] = []
    for path in list_csv_paths(base_dir):
        df = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
            engine="python",
        )
        payloads.append(
            {
                "df": df,
                "source_file": path.name,
                "source_path": str(path),
            }
        )
    return payloads
