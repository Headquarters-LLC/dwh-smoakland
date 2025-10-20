# src/io/storage_base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Any


class StorageBase(ABC):
    """
    Minimal contract for input storages.
    Concrete implementations (Local, Drive, S3, etc.) must implement this.
    """

    @abstractmethod
    def list_files(self, extension: str = "") -> List[str]:
        """
        List absolute or relative paths in the storage that match
        the extension (e.g. '.csv'). If extension == '' return all files.
        """
        raise NotImplementedError

    @abstractmethod
    def read_file(self, path: str) -> Any:
        """
        Read the resource at 'path' and return an appropriate object
        (e.g. a DataFrame if it is CSV). Exact behavior depends on storage.
        """
        raise NotImplementedError
