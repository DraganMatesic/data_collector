"""Database driver pre-flight checks."""

from __future__ import annotations

from data_collector.utilities.functions import runtime


def check_pyodbc() -> None:
    """Validate pyodbc availability."""
    if not runtime.is_module_available("pyodbc"):
        raise ImportError("pyodbc is required but not installed. Install it with `pip install pyodbc`.")
