"""Database column helpers shared by ORM table modules."""

from __future__ import annotations

from typing import Any

from sqlalchemy import BigInteger, Column, Identity, Sequence

from data_collector.settings.main import DatabaseType, MainDatabaseSettings


def auto_increment_column(
    database_type: DatabaseType | None = None,
    primary_key: bool = True,
    **col_kw: Any,
) -> Column[Any]:
    """Return an auto-incrementing BigInteger column for the active backend."""
    if database_type is None:
        main_db_settings = MainDatabaseSettings()
        database_type = main_db_settings.database_type

    if database_type is DatabaseType.POSTGRES:
        return Column(BigInteger, Identity(always=True), primary_key=primary_key, **col_kw)
    if database_type is DatabaseType.ORACLE:
        return Column(BigInteger, Sequence("SEQ_%(column_0_name)s"), primary_key=primary_key, **col_kw)
    return Column(BigInteger, autoincrement=True, primary_key=primary_key, **col_kw)
