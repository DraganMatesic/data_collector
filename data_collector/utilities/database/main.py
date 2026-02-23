"""Database abstraction, connectors, merge workflow, and dependency mapping."""

import logging
import re
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import Column, String, and_, create_engine, select, text
from sqlalchemy.engine import Engine, Result
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Query, Session, declared_attr, sessionmaker
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.sql.visitors import traverse

from data_collector.settings.main import (
    AuthMethods,
    DatabaseDriver,
    DatabaseSettings,
    DatabaseType,
    MainDatabaseSettings,
)
from data_collector.utilities.database.columns import auto_increment_column as auto_increment_column
from data_collector.utilities.database.models import BaseModel as BaseModel
from data_collector.utilities.functions import runtime

logger = logging.getLogger(__name__)

__all__ = [
    "auto_increment_column",
    "BaseModel",
    "BaseDBConnector",
    "Database",
    "MsSQL",
    "Postgres",
    "SHAHashableMixin",
    "Stats",
    "database_classes",
]


def extract_models_from_query(query: Query[Any]) -> set[type[Any]]:
    models: set[type[Any]] = set()

    # Step 1: Extract explicitly queried models via public API
    try:
        for desc in query.column_descriptions:
            entity = desc.get("entity")
            if entity is None:
                continue
            if isinstance(entity, AliasedClass):
                models.add(entity._sa_class_manager.class_)
            elif isinstance(entity, type) and hasattr(entity, "__table__"):
                models.add(entity)
    except Exception as e:
        logger.warning("extract_models: column_descriptions parsing failed: %s", e)

    # Step 2: Traverse SQL tree and find FromClause with mappers
    def visit(element: Any) -> None:
        try:
            mapper = inspect(element, raiseerr=False)
            if mapper and hasattr(mapper, "class_"):
                models.add(mapper.class_)
        except Exception:
            pass

    # Traverse from the actual .statement (Select object)
    try:
        traverse(query.statement, {}, {"clause": visit})
    except Exception as e:
        logger.warning("extract_models: statement traversal failed: %s", e)

    return models


@dataclass
class Stats:
    inserted: int = 0
    archived: int = 0
    deleted: int = 0
    updated: int = 0
    number_of_records: int = 0


class BaseDBConnector(ABC):
    """
    Base Database Connector for all supported DB's
    """
    def __init__(self, settings: DatabaseSettings):
        self.auth_type: AuthMethods = settings.auth_type
        self.settings = settings
        self.settings_class = settings.__class__.__name__
        self.database_name = settings.database_name

        drivers_requiring_dbname = [DatabaseDriver.POSTGRES, DatabaseDriver.ODBC]
        if settings.database_driver in drivers_requiring_dbname and not self.database_name:
            raise ValueError(
                f"database_name must be defined in {self.settings_class} "
                f"setting for drivers {[x.value for x in drivers_requiring_dbname]}."
            )

        self.conn_string = self.build_conn_string()


    @abstractmethod
    def build_conn_string(self) -> str:
        """Build database connection string for target backend."""
        raise NotImplementedError

    def get_host(self) -> str:
        # Checks if it will use ip:port or server name based on available env variables
        ip = self.settings.ip
        port = self.settings.port
        servername = self.settings.server_name

        if ip and port:
            return f"{ip}:{port}"
        elif servername:
            return servername
        else:
            raise ValueError(
                f"Missing host configuration for {self.settings_class} settings. "
                f"Define either ip and port, or server_name."
            )

class Postgres(BaseDBConnector):
    def build_conn_string(self) -> str:
        host = self.get_host()

        # WINDOWS AUTHENTICATION
        if self.auth_type == AuthMethods.WINDOWS:
            return f"postgresql+psycopg2://@{host}/{self.database_name}?gssencmode={self.settings.gssapi.value}"

        # KERBEROS AUTHENTICATION
        elif self.auth_type == AuthMethods.KERBEROS:
            # If username is not provided it will use current system user
            username = '' if self.settings.username is None else self.settings.username
            return f"postgresql+psycopg2://{username}@{host}/{self.database_name}?krbsrvname={self.settings.principal_name}"

        # SQL USERNAME + PASSWORD AUTHENTICATION
        else:
            return f"postgresql+psycopg2://{self.settings.username}:{self.settings.password}@{host}/{self.database_name}"



class MsSQL(BaseDBConnector):
    def build_conn_string(self) -> str:
        host = self.get_host()

        # Checks if user want so use different ODBC Driver
        odbc_driver = self.settings.odbc_driver

        # WINDOWS AUTHENTICATION
        if self.auth_type == AuthMethods.WINDOWS:
            if self.settings.database_driver != DatabaseDriver.ODBC:
                raise ValueError("Windows Authentication requires lib pyodbc.")
            return f"mssql+pyodbc://@{host}/{self.database_name}?trusted_connection=yes&driver={odbc_driver}"
        # SQL USERNAME + PASSWORD AUTHENTICATION
        else:
            # Make connection string using username and password for pymssql
            if self.settings.database_driver == DatabaseDriver.ODBC:
                return f"mssql+pyodbc://{self.settings.username}:{self.settings.password}@{host}/{self.database_name}?driver={odbc_driver}"
            else:
                raise ValueError(f"Unsupported driver '{self.settings.database_driver.value}' for MsSQL.")


def database_classes(db_type: DatabaseType) -> type[BaseDBConnector]:
    _map: dict[DatabaseType, type[BaseDBConnector]] = {
        DatabaseType.POSTGRES: Postgres,
        DatabaseType.MSSQL: MsSQL,
    }
    return _map[db_type]


class Database:
    def __init__(self, settings: DatabaseSettings, app_id: str | None = None, **kwargs: Any) -> None:
        """
        Initializes a database interface with SQLAlchemy engine and optional object mapping.

        Args:
            settings (DatabaseSettings): Configuration instance that inherits from `DatabaseSettings`,
                containing connection parameters like host, port, authentication, and driver info.

            app_id (str, optional): A hashed application identifier assigned by `manager.py` and stored
                in the `apps` table in the database. It is required when `map_objects=True` in settings.

                This ID is used to track database object dependencies dynamically. Provide it:
                - When new database objects (tables, views, routines) may be added or removed
                - When you want automatic registration of those dependencies for analytics or orchestration

                If you're running in a static or read-only context, or object tracking isn't required,
                omit this for better performance.

            **kwargs: Additional keyword arguments passed to SQLAlchemy's `create_engine()`.

        """
        self.settings = settings
        self.settings_class = settings.__class__.__name__

        # App_id of caller
        self.app_id: str | None = app_id

        # Default logger
        self.logger: logging.Logger = logging.getLogger(__name__)

        # Lazily initialized system DB connection for dependency registration
        self._system_db: Database | None = None

        # Construct engine
        self.engine: Engine = self.engine_construct(**kwargs)

    def engine_construct(self, **kwargs: Any) -> Engine:
        """
        Constructs and returns a SQLAlchemy Engine.

        Args:
            **kwargs: Additional keyword arguments passed to SQLAlchemy's `create_engine()`.

        Returns:
            Engine: A SQLAlchemy Engine instance ready for connections.
        """
        database_class = database_classes(self.settings.database_type)
        db_instance = database_class(self.settings)
        return create_engine(db_instance.conn_string, pool_size=20, max_overflow=0, **kwargs)

    def start_session(self) -> Session:
        warnings.warn(
            "'start_session()' is deprecated. Use 'create_session()' instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return self.create_session()

    def create_session(self) -> Session:
        """
        Initializes a new SQLAlchemy session.

        Recommended usage::

            with db.create_session() as session:
                # your queries here
                ...

        Returns:
            Session: SQLAlchemy session object.
        """
        return sessionmaker(self.engine)()

    def merge(
            self,
            objs: Any,
            session: Session,
            filters: Any | None = None,
            archive_col: str = 'archive',
            delete: bool = False,
            update: bool = True,
            stats: bool = False,
            archive_date: datetime | None = None,
            logger: logging.Logger | None = None,
            compare_key: str | list[str] | tuple[str, ...] = 'sha'
    ) -> Stats | None:
        """
        Synchronizes a list of ORM objects (from the web) with the corresponding table in the database.

        This function:
        - Inserts new records found in `objs` that do not exist in the database.
        - Archives or deletes records found in the database that are not present in `objs`.
        - Infers the database table automatically from the ORM class of the first object.

        Args:
            objs:
                A single ORM object or list of ORM objects
                (typically from an external source like a web page, API, or file).
                Each object must have a unique field (default: 'sha') for comparison.

            session (sqlalchemy.orm.Session):
                The SQLAlchemy session to use

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
                If True, records missing from `objs` will have their `archive_col`
                updated with the current time or `archive_date`.
                Mutually exclusive with `delete`.

            stats (bool, optional):
                If True, returns a Stats object summarizing the number of inserts,
                deletions/archives, and total records processed.

            archive_date (datetime, optional):
                A specific datetime to use when marking records as archived.
                If not provided, the current time is used.

            logger (logging.Logger, optional):
                Logger instance used for this call only (e.g. duplicate comparison key warnings).
                Falls back to the instance logger if not provided. Does not mutate instance state.

            compare_key (Union[str, List[str], Tuple[str, ...]], optional):
                One or more attribute names (str or list/tuple of str) used to
                uniquely identify each object. The field is used to determine
                uniqueness (default is 'sha') for comparison between new and
                existing objects. If sha column doesn't exist list 1 or more
                columns names that are used for comparison of new and old objects

        Returns:
            Optional[Stats]: A Stats object with counts for inserted, archived, deleted, and total records.
                             Returns None if `stats=False`.

        Raises:
            AttributeError: If objects do not contain the specified `compare_key`.
        """
        log = logger or self.logger

        # No input data = nothing to do
        if not objs:
            return Stats() if stats else None

        # Normalize to list
        obj_list: list[Any] = [objs] if not isinstance(objs, (list, tuple)) else [*objs]

        # Refuse lists of primitives early
        if all(isinstance(o, str) for o in obj_list):
            raise TypeError(
                "merge() received possibly a list of hash strings."
                "Pass the original ORM objects, or call hash_list(..., inplace=True)."
            )

        # Registering models
        if self.settings.map_objects and self.app_id:
            models_used = self._track_models_from_objects(obj_list)
            self.register_models(models_used)

        # Infer db_table from first ORM object if not explicitly provided
        db_table: type[Any] = obj_list[0].__class__

        # Fetch existing records from the database
        stmt: Any = select(db_table)
        if filters is not None:
            stmt = stmt.filter(filters)
        result = session.execute(stmt)
        db_data = result.scalars().all()

        # Compute differences by compare_key (default 'sha')
        to_insert, to_remove = runtime.obj_diff(
            new_objs=obj_list,
            existing_objs=db_data,
            compare_key=compare_key,
            logger=log
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
                number_of_records=len(obj_list)
            )
        return None

    def delete(self, object_list: list[Any], session: Session) -> None:
        """
        Deletes a list of ORM objects from the database.
        Caller must commit afterward.
        """
        if not object_list:
            return

        for obj in object_list:
            session.delete(obj)

        if self.settings.map_objects and self.app_id:
            models_used = self._track_models_from_objects(object_list)
            self.register_models(models_used)

    def archive(
        self,
        object_list: list[Any],
        session: Session,
        archive_col: str = 'archive',
        archive_date: datetime | None = None,
    ) -> None:
        """
        Updates archive_col on existing DB records with a timestamp.
        """
        if not object_list:
            return

        archive_time = archive_date or datetime.now(UTC)

        for obj in object_list:
            setattr(obj, archive_col, archive_time)
            session.add(obj)

        if self.settings.map_objects and self.app_id:
            models_used = self._track_models_from_objects(object_list)
            self.register_models(models_used)

    def bulk_insert(self, object_list: list[Any], session: Session) -> None:
        """
        Performs a bulk insert of ORM objects using add_all.
        Caller is responsible for committing.
        """
        if object_list:
            session.add_all(object_list)

            if self.settings.map_objects and self.app_id:
                models_used = self._track_models_from_objects(object_list)
                self.register_models(models_used)

    def update_insert(
            self,
            objects: Any,
            session: Session,
            filter_cols: list[str],
            commit: bool = True
    ) -> Stats:
        """
        Performs update or insert of rows based on whether filter column values are matched.

        Args:
            objects: A single ORM object or list/tuple of ORM objects.
            session: The SQLAlchemy session to use.
            filter_cols: Column names used to match existing records.
            commit: If True (default), commits the session after all operations.

        Returns:
            Stats with counts for inserted, updated, and total records.
        """
        obj_list: list[Any] = [objects] if not isinstance(objects, (list, tuple)) else [*objects]

        if not obj_list:
            return Stats()

        stats = Stats(number_of_records=len(obj_list))
        db_table: type[Any] = obj_list[0].__class__
        tracked_models: set[type[Any]] = set()

        for obj in obj_list:
            filters = and_(*[(getattr(db_table, col) == getattr(obj, col)) for col in filter_cols])
            existing_records = self.query(session, db_table, map_objects=True, track_models=tracked_models).filter(
                filters).all()

            if not existing_records:
                session.add(obj)
                stats.inserted += 1
            else:
                for record in existing_records:
                    changed = False
                    for col, value in cast(dict[str, Any], vars(obj)).items():
                        if col == "_sa_instance_state" or not hasattr(record, col):
                            continue
                        current_value = getattr(record, col)
                        if current_value != value:
                            setattr(record, col, value)
                            changed = True
                    if changed:
                        session.add(record)
                        stats.updated += 1

        # Register once per batch
        if tracked_models and self.app_id:
            self.register_models(tracked_models)

        if commit:
            session.commit()

        return stats

    def query(
            self,
            session: Session,
            *models: type[Any],
            map_objects: bool = True,
            track_models: set[type[Any]] | None = None
    ) -> Query[Any]:
        query_obj: Query[Any] = session.query(*models)

        if map_objects and self.settings.map_objects:
            extracted_models: set[type[Any]]
            try:
                extracted_models = extract_models_from_query(query_obj)
            except Exception as e:
                self.logger.warning("Failed to extract models: %s", e)
                extracted_models = set()

            all_models: set[type[Any]] = set(models) | extracted_models

            if track_models is not None:
                track_models.update(all_models)
            elif self.app_id:
                self.register_models(all_models)

        return query_obj

    def execute(self, sql_text: str, session: Session) -> Result[Any]:
        """
        Executes only stored procedures, functions, or package procedures.
        Automatically registers used routine in AppDbObjects.

        Raises:
            ValueError if SQL is not a supported routine call.
        """
        if not self.settings.map_objects or not self.app_id:
            return session.execute(text(sql_text))

        # STEP 1: Validate allowed usage
        pattern = re.compile(r"""
            (?P<keyword>call|exec(?:ute)?|select|begin)  # match starting keyword
            \s+
            (?P<full_name>[\w\.]+)                       # match name: [pkg.]schema.fn
        """, re.IGNORECASE | re.VERBOSE)

        match = pattern.search(sql_text.strip())
        if not match:
            raise ValueError("Only procedure/function/package calls are allowed in db.execute().")

        full_name = match.group("full_name")
        parts = full_name.split(".")
        name, schema = parts[-1], parts[-2] if len(parts) > 1 else None

        # STEP 2: Determine type (function/procedure/package)
        routine_type = self.get_routine_type(name, schema=schema)
        if routine_type.lower() not in ("function", "procedure"):
            raise ValueError(f"Unsupported or unknown routine type for {full_name}")

        # Preparing data for registration
        db_name_override, resolved_schema = self.extract_database_and_schema(schema or "")
        record_data = {
            "app_id": self.app_id,
            "server_name": self.settings.server_name,
            "server_ip": self.settings.ip,
            "database_name": db_name_override or self.settings.database_name,
            "database_schema": resolved_schema,
            "object_name": name,
            "object_type": routine_type.lower()
        }
        self.register_sql_objects([record_data])
        return session.execute(text(sql_text))


    """
    From here are class method that are used for mapping database objects during execution
    """

    def _get_system_db(self) -> "Database":
        """Returns a cached Database instance connected to the main system database."""
        if self._system_db is None:
            self._system_db = Database(MainDatabaseSettings())
        return self._system_db

    def _track_models_from_objects(self, objects: list[Any] | tuple[Any, ...]) -> set[type[Any]]:
        """
        Collects distinct ORM model classes from the given object list.
        """
        return {obj.__class__ for obj in objects if hasattr(obj, "__class__")}

    def get_model_source_type(self, model: Any) -> str:
        """
        Tries to identify whether the model is mapped to a table, view,
        function, or procedure. Defaults to 'unknown' if not identifiable.
        """
        try:
            table_name = model.__table__.name
            schema = self.get_schema_for_model(model)
            inspector = inspect(self.engine)

            # 1. Check for views
            views = inspector.get_view_names(schema=schema)
            if table_name in views:
                return "view"

            # 2. Check for tables
            tables = inspector.get_table_names(schema=schema)
            if table_name in tables:
                return "table"

            # 3. Fallback: try routine (function or procedure)
            routine_type = self.get_routine_type(table_name, schema=schema)
            if routine_type.lower() in ("function", "procedure"):
                return routine_type.lower()

            # 4. Unknown type
            return "unknown"

        except Exception as e:
            self.logger.warning("get_model_source_type failed for %s: %s", model.__name__, e)
            return "unknown"

    def get_routine_type(self, object_name: str, schema: str | None = None) -> str:
        """
        Returns the type of routine (procedure/function) in the connected database.

        - For PostgreSQL: queries information_schema.routines
        - For MSSQL: queries sys.objects

        Returns: 'FUNCTION', 'PROCEDURE', or 'UNKNOWN'
        """
        db_type = self.settings.database_type

        with self.engine.connect() as conn:
            if db_type == DatabaseType.POSTGRES:
                query = """
                SELECT routine_type
                FROM information_schema.routines
                WHERE routine_name = :name
                AND routine_schema = :schema
                """
                result = conn.execute(
                    text(query), {"name": object_name, "schema": schema or "public"}
                ).fetchone()
                return result[0].upper() if result else "unknown"

            elif db_type == DatabaseType.MSSQL:
                query = """
                SELECT type_desc
                FROM sys.objects
                WHERE name = :name
                AND type IN ('P', 'FN', 'IF', 'TF')
                """
                result = conn.execute(text(query), {"name": object_name}).fetchone()
                if result:
                    # Normalize MSSQL terms
                    td = result[0].lower()
                    if "procedure" in td:
                        return "PROCEDURE"
                    elif "function" in td:
                        return "FUNCTION"
                return "unknown"

            else:
                raise NotImplementedError(f"Routine type check not implemented for {db_type}")

    def get_schema_for_model(self, db_object: Any) -> str:
        """
        Returns the schema for a given model, with DBMS-specific fallbacks.
        """
        schema = db_object if isinstance(db_object, str) else db_object.__table__.schema

        if schema:
            return schema

        if self.settings.database_type == DatabaseType.POSTGRES:
            return "public"
        elif self.settings.database_type == DatabaseType.MSSQL:
            return "dbo"
        else:
            return "unknown"

    def extract_database_and_schema(self, db_object: Any) -> tuple[str | None, str]:
        """
        Extracts (database_name, schema_name) from a model's __table__.schema definition.
        Handles special formats like 'OtherDB..' in MSSQL, where '..' implies default schema (dbo).
        """
        schema = db_object if isinstance(db_object, str) else db_object.__table__.schema

        if schema and '.' in schema:
            parts = schema.split('.')

            # Case: 'OtherDB..' â†’ ['', '', ...] or ['OtherDB', '', 'SomeTable']
            if len(parts) == 2:
                db_name, schema_part = parts
                schema_part = schema_part or self.get_schema_for_model(db_object)
                return db_name, schema_part

            elif len(parts) > 2:
                db_name = parts[0]
                schema_part = parts[1] or self.get_schema_for_model(db_object)
                return db_name, schema_part

        # No cross-db prefix â†’ use fallback logic
        return None, self.get_schema_for_model(db_object)


    def register_models(self, db_objects: set[type[Any]]) -> None:
        """
        Registers ORM objects
        """
        from data_collector.tables import AppDbObjects
        system_db = self._get_system_db()
        with system_db.create_session() as session:
            dependencies: list[Any] = []
            dependencies_hash: list[str] = []

            for db_object in db_objects:
                try:
                    object_name = db_object.__table__.name
                    db_name_override, schema = self.extract_database_and_schema(db_object)
                    object_type = self.get_model_source_type(db_object)

                    # Source info: comes from the engine tied to this model's session (self)
                    database_name = db_name_override or self.settings.database_name or ""

                    # Prepare data for ORM object
                    record_data = self.prepare_dependency_record(
                        object_name=object_name,
                        object_type=object_type,
                        schema=schema,
                        database_name=database_name,
                    )
                    record_hash = cast(str, record_data["sha"])

                    # Create AppDbObjects entry
                    if record_hash not in dependencies_hash:
                        dependency = AppDbObjects(**record_data)
                        dependencies.append(dependency)
                        dependencies_hash.append(record_hash)

                except Exception as e:
                    self.logger.error(f"[register_models] Failed to register {db_object.__name__}: {e}")

            if dependencies:
                filters = and_(AppDbObjects.app_id == self.app_id)
                system_db.merge(dependencies, session, filters=filters)


    def register_sql_objects(self, objects: list[dict[str, Any]]) -> None:
        """
        Registers non-ORM objects (from raw SQL) using same logic as register_models.
        """
        from data_collector.tables import AppDbObjects
        system_db = self._get_system_db()

        with system_db.create_session() as session:
            dependencies: list[Any] = []
            dependencies_hash: list[str] = []

            for obj in objects:
                try:
                    object_name = obj["object_name"]
                    schema = obj["database_schema"] or self.get_schema_for_model(obj["database_schema"] )
                    database_name = cast(str | None, obj["database_name"]) or self.settings.database_name or ""
                    object_type = obj["object_type"]

                    # Verify actual type if needed
                    if object_type in {"function", "procedure"}:
                        object_type = self.get_routine_type(object_name, schema).lower()
                    elif object_type == "table":
                        inspector = inspect(self.engine)
                        if object_name in inspector.get_view_names(schema=schema):
                            object_type = "view"

                    record_data = self.prepare_dependency_record(
                        object_name=object_name,
                        object_type=object_type,
                        schema=schema,
                        database_name=database_name,
                    )
                    record_hash = cast(str, record_data["sha"])

                    if record_hash not in dependencies_hash:
                        dependencies.append(AppDbObjects(**record_data))
                        dependencies_hash.append(record_hash)
                except Exception as e:
                    self.logger.error(f"[register_sql_objects] Failed to register {obj}: {e}")

            if dependencies:
                filters = and_(AppDbObjects.app_id == self.app_id)
                system_db.merge(dependencies, session, filters=filters)

    def prepare_dependency_record(
            self,
            *,
            object_name: str,
            object_type: str,
            schema: str,
            database_name: str | None
    ) -> dict[str, Any]:
        """
        Prepares a fully hashed and timestamped dictionary for AppDbObjects registration.
        """
        record_data: dict[str, Any] = {
            "app_id": self.app_id,
            "server_type": self.settings.database_type.value,
            "server_name": self.settings.server_name,
            "server_ip": self.settings.ip,
            "database_name": database_name or "",
            "database_schema": schema,
            "object_name": object_name,
            "object_type": object_type
        }

        record_hash = str(runtime.make_hash(record_data))
        record_data.update({"sha": record_hash, "last_use_date": datetime.now(UTC)})
        return record_data


class SHAHashableMixin:
    @declared_attr
    def sha(cls) -> Column[Any]:
        return Column(String(64), index=True)

    def get_fields(self) -> dict[str, Any]:
        """Return only public attributes for hashing."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

    @staticmethod
    @abstractmethod
    def get_hash_keys() -> list[str]:
        """Returns a list of field names to be hashed. Must be overridden in child class."""
        ...

    def compute_sha(self) -> str:
        """
        Computes the SHA value based on defined keys and fields.
        """
        return cast(str, runtime.make_hash(self.get_fields(), on_keys=self.get_hash_keys()))

    def __init__(self, **kwargs: Any) -> None:
        auto_sha: bool = kwargs.pop("auto_sha", True)
        super().__init__(**kwargs)
        if auto_sha and not getattr(self, "sha", None):
            self.sha = self.compute_sha()
