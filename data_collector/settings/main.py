from enum import Enum
from pydantic import Field
from typing import Optional
from pydantic_settings import BaseSettings


class DatabaseType(Enum):
    POSTGRES = "Postgres"
    MSSQL = "MsSQL"
    ORACLE = "Oracle"


class DatabaseDriver(str, Enum):
    POSTGRES = 'psycopg2'
    ORACLE = 'oracledb'
    ODBC = 'pyodbc'

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


class AuthMethods(str, Enum):
    SQL = 'sql'
    WINDOWS = 'windows'
    KERBEROS = 'kerberos'


class GssApiEnc(Enum):
    DISABLE = 'disable' # Don't use GSS encryption, even if available
    PREFER = 'prefer' # Use GSS encryption if the server supports it (default)
    REQUIRE = 'require' # Fail the connection if GSS encryption cannot be used



class DatabaseSettings(BaseSettings):
    # Basic database authentication
    username: str = None
    password: str = None

    # Required for PostgreSQL and MSSQL databases
    database_name: Optional[str] = None
    database_type: DatabaseType
    database_driver: DatabaseDriver

    # For creating host ip and port or server_name is needed
    # If both are provided it will prioritise ip:port over server_name
    ip: Optional[str] = None
    port: Optional[int] = None
    server_name: Optional[str] = None

    # Auth type on wanted database server
    # sql (username and password) all DB's
    # windows (Windows authentication) - PostgreSQL and MSSQL
    # kerberos (Kerberos authentication) - PostgreSQL
    auth_type: AuthMethods = AuthMethods.SQL

    # psycopg2 option for Postgresql
    # gssapi means Whether the connection is encrypted using GSSAPI-level encryption (separate from SSL)
    gssapi: GssApiEnc = GssApiEnc.PREFER

    # Kerberos service principal name that will be used for auth
    principal_name: str = 'postgres'

    # MSSQL Windows ODBC driver to use
    odbc_driver: str = 'ODBC+Driver+17+for+SQL+Server'

    # SID (System Identifier) Oracle database instance name
    sidname: Optional[str] = None

    # Flag that enables mapping of database object of apps that depends on them during execution
    map_objects: Optional[bool] = False



class MainDatabaseSettings(DatabaseSettings):
    """
    Settings for database where data_collector will deploy its database objects
    """
    username: str = Field(..., alias="DC_DB_MAIN_USERNAME")
    password: str = Field(..., alias="DC_DB_MAIN_PASSWORD")
    database_name: str = Field(..., alias="DC_DB_MAIN_DATABASENAME")
    ip: Optional[str] = Field(..., alias="DC_DB_MAIN_IP")
    port: Optional[int] = Field(..., alias="DC_DB_MAIN_PORT")
    server_name: Optional[str] = Field(None, alias="DC_DB_MAIN_SERVERNAME")
    database_type: DatabaseType = DatabaseType.POSTGRES
    database_driver: DatabaseDriver = DatabaseDriver.POSTGRES
    map_objects: Optional[bool] = True


class GeneralSettings(BaseSettings):
    db_main: MainDatabaseSettings = MainDatabaseSettings()

