import os
from enum import Enum
from datetime import datetime
from dataclasses import dataclass
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data_collector.utilities import env
from sqlalchemy.ext.declarative import declared_attr
from data_collector.utilities.functions import runtime
from data_collector.utilities.database import loaders

from sqlalchemy import (
    text, select, Column, String
)

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
        self.session = self.start_session()

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

    def merge(
            self,
            objs,
            filters=text(''),
            archive_col:str='archive',
            delete:bool=False,
            update:bool=True,
            session=None,
            stats:bool=False,
            archive_date=None,
            logger=None,
            compare_key='sha'
    ):
        """
        Synchronizes a list of ORM objects (from the web) with the corresponding table in the database.

        This function:
        - Inserts new records found in `objs` that do not exist in the database.
        - Archives or deletes records found in the database that are not present in `objs`.
        - Infers the database table automatically from the ORM class of the first object.

        Args:
            objs (Union[object, List[object]]):
                A single ORM object or list of ORM objects (typically from an external source like an WEB, API, File).
                Each object must have a unique field (default: 'sha') for comparison.

            filters (sqlalchemy.sql.elements.TextClause, optional):
                SQLAlchemy filter to narrow the selection of records from the database.
                If not provided, all records from the inferred table will be fetched.

            archive_col (str, optional):
                The name of the column used to mark a record as archived.
                Archived record = history record no longer active on source.
                Default is 'archive'. Only relevant if `update=True`.

            delete (bool, optional):
                If True, records missing from `objs` will be permanently deleted from the database.
                Mutually exclusive with `update`.

            update (bool, optional):
                If True, records missing from `objs` will have their `archive_col` updated with the current time or `archive_date`.
                Mutually exclusive with `delete`.

            session (sqlalchemy.orm.Session, optional):
                The SQLAlchemy session to use. If not provided, falls back to `self.session`.

            stats (bool, optional):
                If True, returns a Stats object summarizing the number of inserts, deletions/archives, and total records processed.

            archive_date (datetime, optional):
                A specific datetime to use when marking records as archived.
                If not provided, the current time is used.

            logger (logging.Logger, optional):
                Logger instance for reporting warnings such as duplicate comparison keys.

            compare_key (str, optional):
                The field used to determine uniqueness (usually 'sha') for comparison between new and existing records.

        Returns:
            Optional[Stats]: A Stats object with counts for inserted, archived, deleted, and total records.
                             Returns None if `stats=False`.

        Raises:
            AttributeError: If objects do not contain the specified `compare_key`.
        """

        # No input data = nothing to do
        if not objs:
            return Stats(0, 0, 0, 0) if stats else None

        # Normalize to list
        if not isinstance(objs, (list, tuple)):
            objs = [objs]

        # Infer db_table from first ORM object if not explicitly provided
        db_table = objs[0].__class__

        # Fallback to default session if not provided
        session = session or self.session

        # Fetch existing records from the database
        result = session.execute(select(db_table).filter(filters))
        db_data = result.scalars().all()

        # Compute differences by compare_key (default 'sha')
        to_insert, to_remove = runtime.obj_diff(
            new_objs=objs,
            existing_objs=db_data,
            compare_key=compare_key,
            logger=logger
        )

        # Process deletions or archiving
        if delete:
            self.delete(to_remove, session)
        elif update:
            self.archive(to_remove, session=session, archive_col=archive_col, archive_date=archive_date)

        # Insert new records
        self.bulk_insert(to_insert, session=session)

        # Commit all changes
        session.commit()

        # Return detailed stats if requested
        if stats:
            return Stats(
                inserted=len(to_insert),
                archived=len(to_remove) if update and not delete else 0,
                deleted=len(to_remove) if delete else 0,
                number_of_records=len(objs)
            )

    def delete(self, object_list, session=None):
        """
        Deletes a list of ORM objects from the database.
        Caller must commit afterward.
        """
        session = session or self.session
        if not object_list:
            return
        for obj in object_list:
            session.delete(obj)

    @staticmethod
    def archive(object_list, session, archive_col='archive', archive_date=None):
        """
        Updates archive_col on existing DB records with a timestamp.
        """
        if not object_list:
            return

        archive_time = archive_date or datetime.now()

        for obj in object_list:
            setattr(obj, archive_col, archive_time)
            session.add(obj)

    def bulk_insert(self, object_list, session=None):
        """
        Performs a bulk insert of ORM objects using add_all.
        Caller is responsible for committing.
        """
        session = session or self.session
        if object_list:
            session.add_all(object_list)


class SHAHashableMixin:
    @declared_attr
    def sha(cls):
        return Column(String(64), index=True)

    def get_fields(self)-> dict:
        """Return only public attributes for hashing."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

    @staticmethod
    def get_hash_keys() -> list:
        """
        Returns a list of field names to be hashed.
        Must be overridden in child class.
        """
        raise NotImplementedError("get_hash_keys() must be implemented by child class.")

    def compute_sha(self) -> str:
        """
        Computes the SHA value based on defined keys and fields.
        """
        return runtime.make_hash(self.get_fields(), on_keys=self.get_hash_keys())

    def __init__(self, *args, auto_sha=True, **kwargs):
        super().__init__(*args, **kwargs)
        if auto_sha and not getattr(self, "sha", None):
            self.sha = self.compute_sha()


class BaseModel:
    def __repr__(self):
        cls = self.__class__.__name__
        if hasattr(self, '__table__'):
            attrs = ', '.join(
                f"{col.name}={getattr(self, col.name)!r}"
                for col in self.__table__.columns
            )
            return f"<{cls}({attrs})>"
        return f"<{cls}>"

    def __str__(self):
        return self.__repr__()