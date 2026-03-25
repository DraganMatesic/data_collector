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
            "id", "function_hash", "log_role", "main_app", "app_id",
            "call_count", "task_size", "solved", "failed",
            "processed_count", "is_success",
            "start_time", "end_time",
            "total_elapsed_ms", "average_elapsed_ms", "median_elapsed_ms",
            "min_elapsed_ms", "max_elapsed_ms",
            "caller_log_id", "runtime", "date_created",
        }
        assert expected.issubset(columns)

    def test_removed_columns_absent(self) -> None:
        columns = {c.name for c in sa_inspect(FunctionLog).columns}
        removed = {
            "execution_order", "thread_execution_order", "parent_log_id",
            "thread_id", "totals", "totalm", "totalh",
        }
        assert removed.isdisjoint(columns)

    def test_unique_constraint_function_hash_runtime(self) -> None:
        constraints = FunctionLog.__table__.constraints
        unique_constraints = [c for c in constraints if hasattr(c, "columns") and len(c.columns) == 2]
        found = any(
            {col.name for col in c.columns} == {"function_hash", "runtime"}
            for c in unique_constraints
        )
        assert found, "UniqueConstraint on (function_hash, runtime) not found"

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

    def test_function_log_error_table_removed(self) -> None:
        from data_collector.tables import log
        assert not hasattr(log, "FunctionLogError"), "FunctionLogError table should be removed"


class TestExports:
    def test_app_functions_exported(self) -> None:
        from data_collector.tables import AppFunctions as Exported
        assert Exported is AppFunctions

    def test_function_log_exported(self) -> None:
        from data_collector.tables import FunctionLog as Exported
        assert Exported is FunctionLog

    def test_function_log_error_not_exported(self) -> None:
        import data_collector.tables as tables_module
        assert not hasattr(tables_module, "FunctionLogError")
