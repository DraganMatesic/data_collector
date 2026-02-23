"""Database driver pre-flight checks and optional Oracle client initialization."""

from __future__ import annotations

import importlib
import logging
import os

from data_collector.utilities.functions import runtime


def initialize_oracle_client(logger: logging.Logger | None = None) -> bool:
    """Initialize Oracle thick mode when `ORACLE_CLIENT` is configured."""
    try:
        oracledb = importlib.import_module("oracledb")
    except ImportError:
        if logger:
            logger.error("oracledb module not installed.")
        return False

    oracle_path = os.getenv("ORACLE_CLIENT")
    if not oracle_path:
        if logger:
            logger.info("No 'ORACLE_CLIENT' env var set. Using oracledb thin mode.")
        return True

    try:
        oracledb.init_oracle_client(lib_dir=oracle_path)
        if logger:
            logger.info("Oracle thick mode initialized from: %s", oracle_path)
        return True
    except Exception as exc:
        if "already initialized" in str(exc):
            if logger:
                logger.info("Oracle client was already initialized.")
            return True
        if logger:
            logger.error("Oracle client initialization failed: %s", exc)
        return False


def check_oracle(logger: logging.Logger | None = None) -> None:
    """Validate Oracle driver availability and optional client initialization."""
    if not runtime.is_module_available("oracledb"):
        raise ImportError("oracledb is required but not installed. Install it with `pip install oracledb`.")

    if not initialize_oracle_client(logger=logger):
        raise RuntimeError("Oracle initialization failed or 'ORACLE_CLIENT' env variable missing.")


def check_pyodbc() -> None:
    """Validate pyodbc availability."""
    if not runtime.is_module_available("pyodbc"):
        raise ImportError("pyodbc is required but not installed. Install it with `pip install pyodbc`.")
