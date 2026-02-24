"""Unit tests for AppFunctions and FunctionLog ORM models."""

from __future__ import annotations

from sqlalchemy import inspect as sa_inspect

from data_collector.tables.apps import AppFunctions
from data_collector.tables.log import FunctionLog


class TestAppFunctions:
    def test_tablename(self) -> None:
        assert AppFunctions.__tablename__ == "app_functions"

    def test_columns_present(self) -> None:
        columns = {c.name for c in sa_inspect(AppFunctions).columns}
        expected = {
            "id", "function_hash", "function_name", "filepath",
            "app_id", "first_seen", "last_seen",
            "sha", "archive", "date_created", "date_modified",
        }
        assert expected.issubset(columns)

    def test_function_hash_unique_and_not_nullable(self) -> None:
        col = AppFunctions.__table__.c.function_hash
        assert col.unique is True
        assert col.nullable is False

    def test_function_name_not_nullable(self) -> None:
        col = AppFunctions.__table__.c.function_name
        assert col.nullable is False

    def test_app_id_foreign_key(self) -> None:
        fks = {fk.target_fullname for fk in AppFunctions.__table__.foreign_keys}
        assert "apps.app" in fks

    def test_app_id_indexed(self) -> None:
        col = AppFunctions.__table__.c.app_id
        assert col.index is True

    def test_function_hash_indexed(self) -> None:
        col = AppFunctions.__table__.c.function_hash
        assert col.index is True


class TestFunctionLog:
    def test_tablename(self) -> None:
        assert FunctionLog.__tablename__ == "function_log"

    def test_columns_present(self) -> None:
        columns = {c.name for c in sa_inspect(FunctionLog).columns}
        expected = {
            "id", "function_hash", "execution_order", "main_app", "app_id",
            "thread_id", "task_size", "solved", "failed",
            "start_time", "end_time", "totals", "totalm", "totalh",
            "runtime", "sha", "archive", "date_created", "date_modified",
        }
        assert expected.issubset(columns)

    def test_function_hash_foreign_key(self) -> None:
        fks = {fk.target_fullname for fk in FunctionLog.__table__.foreign_keys}
        assert "app_functions.function_hash" in fks

    def test_runtime_foreign_key(self) -> None:
        fks = {fk.target_fullname for fk in FunctionLog.__table__.foreign_keys}
        assert "runtime.runtime" in fks

    def test_indexed_columns(self) -> None:
        table = FunctionLog.__table__
        for col_name in ("function_hash", "main_app", "app_id", "runtime"):
            assert table.c[col_name].index is True, f"{col_name} should be indexed"


class TestExports:
    def test_app_functions_exported(self) -> None:
        from data_collector.tables import AppFunctions as Exported
        assert Exported is AppFunctions

    def test_function_log_exported(self) -> None:
        from data_collector.tables import FunctionLog as Exported
        assert Exported is FunctionLog
