import os
import platform
from enum import Enum
from dataclasses import dataclass
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data_collector.utilities import env
from data_collector.utilities.functions import runtime
from data_collector.utilities.database import loaders


class DatabaseDriver(str, Enum):
    POSTGRES = 'psycopg2'
    ORACLE = 'cx_oracle'
    ODBC = 'pyodbc'

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


class DatabaseType(Enum):
    POSTGRES = "Postgres"
    MSSQL = "MsSQL"
    ORACLE = "Oracle"

    def get_class(self):
        return {
            DatabaseType.POSTGRES: Postgres,
            DatabaseType.MSSQL: MsSQL,
            DatabaseType.ORACLE: Oracle
        }[self]


class AuthMethods(str, Enum):
    SQL = 'sql'
    WINDOWS = 'windows'
    KERBEROS = 'kerberos'


@dataclass
class Stats:
    inserted: int = 0
    archived: int = 0
    deleted: int = 0
    number_of_records: int = 0


class BaseDBConnector(ABC):
    """
    Base Database Connector for all supported DB's
    """
    def __init__(self, dbname: str, driver: str):
        auth_value = os.getenv(f"{dbname}_auth_type", "sql").lower()

        self.dbname = dbname
        self.driver = driver
        self.database = os.getenv(f"{self.dbname}_databasename")

        try:
            self.auth_type: AuthMethods = AuthMethods(auth_value)
        except ValueError:
            valid = runtime.list_enum_values(AuthMethods)
            raise ValueError(
                f"Invalid auth_type '{auth_value}' for {dbname}. Must be one of: {valid}"
            )

        if driver in [DatabaseDriver.POSTGRES, DatabaseDriver.ODBC] and not self.database:
            raise ValueError(f"{self.dbname}_databasename must be defined in environment variables.")

        self.conn_string = self.build_conn_string()


    def get_env_params(self, keys):
        return {key: os.getenv(f"{self.dbname}_{key}") for key in keys}

    @abstractmethod
    def build_conn_string(self):
        pass

    def get_host(self) -> str:
        ip = os.getenv(f"{self.dbname}_ip")
        port = os.getenv(f"{self.dbname}_port")
        servername = os.getenv(f"{self.dbname}_servername")

        if ip and port:
            return f"{ip}:{port}"
        elif servername:
            return servername
        else:
            raise ValueError(
                f"Missing host configuration for {self.dbname}. "
                f"Define either {self.dbname}_ip and {self.dbname}_port, or {self.dbname}_servername."
            )


class Postgres(BaseDBConnector):
    def build_conn_string(self):
        host = self.get_host()
        # WINDOWS AUTHENTICATION
        if self.auth_type == AuthMethods.WINDOWS:
            return f"postgresql+psycopg2://@{host}/{self.database}?gssencmode=prefer"

        # KERBEROS AUTHENTICATION
        elif self.auth_type == AuthMethods.KERBEROS:
            params = self.get_env_params(['username'])

            # if username is not provided it will use current system user
            username = params.get('username')
            if username is None:
                username = ''

            return f"postgresql+psycopg2://{username}@{host}/{self.database}?krbsrvname=postgres"

        # SQL USERNAME + PASSWORD AUTHENTICATION
        else:
            params = self.get_env_params(['username', 'password', 'databasename'])
            params.update({'host': host})
            return "postgresql+psycopg2://{username}:{password}@{host}/{databasename}".format(**params)


class MsSQL(BaseDBConnector):
    def build_conn_string(self):
        host = self.get_host()

        # WINDOWS AUTHENTICATION
        if self.auth_type == AuthMethods.WINDOWS:
            if self.driver != DatabaseDriver.ODBC:
                raise ValueError("Windows Authentication requires driver='pyodbc'.")
            return f"mssql+pyodbc://@{host}/{self.database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"

        # SQL USERNAME + PASSWORD AUTHENTICATION
        else:
            params = self.get_env_params(['username', 'password', 'databasename'])
            params.update({'host': host})
            # make connection string using username and password for pymssql
            if self.driver == DatabaseDriver.ODBC:
                return "mssql+pyodbc://{username}:{password}@{host}/{databasename}?driver=ODBC+Driver+17+for+SQL+Server".format(**params)
            else:
                raise ValueError(f"Unsupported driver '{self.driver}' for MsSQL.")


class Oracle(BaseDBConnector):
    def build_conn_string(self):
        unsupported = [AuthMethods.WINDOWS, AuthMethods.KERBEROS]
        if self.auth_type in unsupported:
            raise ValueError(f"{self.auth_type.value} authentication is not supported for Oracle.")
        else:
            params = self.get_env_params(['username', 'password', 'serverip', 'serverport', 'sidname'])
            return "oracle+cx_oracle://{username}:{password}@{serverip}:{serverport}/{sidname}".format(**params)


class Database:
    def __init__(self, dbname, logger,
                 driver: DatabaseDriver = DatabaseDriver.POSTGRES,
                 env_check=False, **kwargs):

        # if flag is true > check if environment variable are loaded
        if env_check:
            env.check(logger=logger)

        # check for Oracle dependencies and library
        if driver == DatabaseDriver.ORACLE:
            loaders.check_oracle(logger)

        # check for MSSQL libraries
        if driver == DatabaseDriver.ODBC:
            loaders.check_pyodbc()

        # construct engine
        self.engine = self.engine_construct(dbname, driver, **kwargs)

    @staticmethod
    def engine_construct(dbname, driver:DatabaseDriver = DatabaseDriver.POSTGRES, **kwargs):
        dbname_type_str = os.getenv(f"{dbname}_type")

        if dbname_type_str is None:
            raise ConnectionError(
                f"Can't find {dbname}_type in env variables. "
                f"Happens when no .env file is present in the virtual environment or variable is missing in .env file."
                f"Create one manually or copy it from .env_example."
            )

        try:
            db_type = DatabaseType(dbname_type_str)
        except ValueError:
            valid = [dt.value for dt in DatabaseType]
            raise ValueError(f"Unsupported db type '{dbname_type_str}' for {dbname}. Valid values: {valid}")

        db_class = db_type.get_class()
        db_instance = db_class(dbname, driver)
        return create_engine(db_instance.conn_string, pool_size=20, max_overflow=0, **kwargs)

    def start_session(self):
        return sessionmaker(self.engine)()

