"""Storage management package for file organization, deduplication, and retention."""

from data_collector.storage.backend import BaseStorageBackend, FilesystemBackend
from data_collector.storage.janitor import StorageJanitor
from data_collector.storage.manager import StorageManager
from data_collector.storage.retention import RetentionPolicy, enforce_retention, enforce_retention_by_database

__all__ = [
    "BaseStorageBackend",
    "FilesystemBackend",
    "RetentionPolicy",
    "StorageJanitor",
    "StorageManager",
    "enforce_retention",
    "enforce_retention_by_database",
]
