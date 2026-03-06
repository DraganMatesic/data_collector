from __future__ import annotations

from sqlalchemy import Text
from sqlalchemy import inspect as sa_inspect

from data_collector.tables.log import Logs


def test_logs_table_contains_expected_columns() -> None:
    columns = {column.name for column in sa_inspect(Logs).columns}
    expected = {
        "id",
        "app_id",
        "module_name",
        "module_path",
        "function_name",
        "function_id",
        "call_chain",
        "thread_id",
        "lineno",
        "log_level",
        "msg",
        "context_json",
        "runtime",
        "date_created",
    }
    assert expected.issubset(columns)


def test_logs_context_json_column_is_nullable_text() -> None:
    column = Logs.__table__.c.context_json
    assert isinstance(column.type, Text)
    assert column.nullable is True


def test_logs_core_columns_are_indexed() -> None:
    table = Logs.__table__
    for column_name in ("app_id", "function_id", "log_level", "runtime"):
        assert table.c[column_name].index is True
