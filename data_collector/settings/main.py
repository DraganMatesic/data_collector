"""Pydantic settings models for database and logging configuration."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field
from pydantic_settings import BaseSettings


class DatabaseType(StrEnum):
    """Supported database backends."""

    POSTGRES = "Postgres"
    MSSQL = "MsSQL"


class DatabaseDriver(StrEnum):
    """Supported DBAPI drivers."""

    POSTGRES = 'psycopg2'
    ODBC = 'pyodbc'

    @classmethod
    def has_value(cls, value: str) -> bool:
        """Return True when the provided driver value exists in enum members."""
        return value in cls._value2member_map_


class AuthMethods(StrEnum):
    """Authentication modes for database connections."""

    SQL = 'sql'
    WINDOWS = 'windows'
    KERBEROS = 'kerberos'


class GssApiEnc(StrEnum):
    """PostgreSQL GSSAPI encryption behavior."""

    DISABLE = 'disable'  # Don't use GSS encryption, even if available
    PREFER = 'prefer'  # Use GSS encryption if the server supports it (default)
    REQUIRE = 'require'  # Fail the connection if GSS encryption cannot be used


class DatabaseSettings(BaseSettings):
    """Base shared database settings model."""

    # Basic database authentication
    username: str | None = None
    password: str | None = None

    # Required for PostgresSQL and MSSQL databases
    database_name: str | None = None
    database_type: DatabaseType
    database_driver: DatabaseDriver

    # For creating host ip and port or server_name is needed
    # If both are provided it will prioritise ip:port over server_name
    ip: str | None = None
    port: int | None = None
    server_name: str | None = None

    # Auth type on wanted database server
    # sql (username and password) all DB's
    # windows (Windows authentication) - PostgresSQL and MSSQL
    # kerberos (Kerberos authentication) - PostgresSQL
    auth_type: AuthMethods = AuthMethods.SQL

    # psycopg2 option for Postgresql
    # gssapi means Whether the connection is encrypted using GSSAPI-level encryption (separate from SSL)
    gssapi: GssApiEnc = GssApiEnc.PREFER

    # Kerberos service principal name that will be used for auth
    principal_name: str = 'postgres'

    # MSSQL Windows ODBC driver to use
    odbc_driver: str = 'ODBC+Driver+17+for+SQL+Server'

    # Flag that enables mapping of database object of apps that depends on them during execution
    map_objects: bool | None = False


class MainDatabaseSettings(DatabaseSettings):
    """Settings for database where data_collector deploys framework objects."""

    username: str | None = Field(default=None, alias="DC_DB_MAIN_USERNAME")
    password: str | None = Field(default=None, alias="DC_DB_MAIN_PASSWORD")
    database_name: str | None = Field(default=None, alias="DC_DB_MAIN_DATABASENAME")
    ip: str | None = Field(default=None, alias="DC_DB_MAIN_IP")
    port: int | None = Field(default=None, alias="DC_DB_MAIN_PORT")
    server_name: str | None = Field(default=None, alias="DC_DB_MAIN_SERVERNAME")
    database_type: DatabaseType = DatabaseType.POSTGRES
    database_driver: DatabaseDriver = DatabaseDriver.POSTGRES
    map_objects: bool | None = True


class LogSettings(BaseSettings):
    """Cross-cutting logging behavior settings."""

    log_to_db: bool = True
    log_to_splunk: bool = False
    splunk_hec_url: str | None = None
    splunk_token: str | None = None
    log_max_queue: int = 10000


class GeneralSettings(BaseSettings):
    """General settings is used when more than one setting is required to be imported into app"""

    @staticmethod
    def _default_main_db() -> MainDatabaseSettings:
        return MainDatabaseSettings()

    db_main: MainDatabaseSettings = Field(default_factory=_default_main_db)
    log_settings: LogSettings = LogSettings()
