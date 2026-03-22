"""Database abstraction, connectors, merge workflow, and dependency mapping."""

import logging
import re
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import Column, Select, String, and_, create_engine, select, text
from sqlalchemy.engine import Engine, Result
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session, declared_attr, sessionmaker
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.expression import Executable

from data_collector.settings.main import (
    AuthMethods,
    DatabaseDriver,
    DatabaseSettings,
    DatabaseType,
    MainDatabaseSettings,
)
from data_collector.tables.apps import AppDbObjects
from data_collector.utilities.database.columns import auto_increment_column as auto_increment_column
from data_collector.utilities.database.models import BaseModel as BaseModel
from data_collector.utilities.functions import runtime

logger = logging.getLogger(__name__)

_NO_MODELS_WARNING = (
    "No ORM models detected in statement. Use models= parameter for explicit "
    "tracking or restructure the query to use a database view/procedure."
)

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


def extract_models_from_statement(statement: Executable) -> set[type[Any]]:
    """Extract ORM model classes from a SQLAlchemy 2.x statement.

    Recursively traverses Select, Update, Delete, and CompoundSelect statements
    to find all ORM-mapped models, including those in joins, select_from, aliased
    models, subqueries, EXISTS clauses, unions, and cross-table WHERE references.

    Args:
        statement: A SQLAlchemy 2.x executable (select, update, delete, union, etc.).

    Returns:
        Set of ORM model classes referenced in the statement.
    """
    models: set[type[Any]] = set()
    visited: set[int] = set()
    _extract_from_construct(statement, models, visited)
    return models


def _extract_from_construct(construct: Any, models: set[type[Any]], visited: set[int]) -> None:
    """Recursively extract ORM models from a SQLAlchemy construct."""
    construct_id = id(construct)
    if construct_id in visited:
        return
    visited.add(construct_id)

    # Step 1: column_descriptions (Select statements)
    try:
        descriptions: list[dict[str, Any]] = getattr(construct, "column_descriptions", [])
        for description in descriptions:
            entity = description.get("entity")
            if entity is None:
                continue
            if isinstance(entity, type) and hasattr(entity, "__table__"):
                models.add(entity)
            elif isinstance(entity, AliasedClass):
                aliased_class: type[Any] | None = getattr(inspect(entity), "class_", None)  # type: ignore[arg-type]
                if aliased_class is not None:
                    models.add(aliased_class)
    except Exception as extraction_error:
        logger.warning("extract_models: column_descriptions parsing failed: %s", extraction_error)

    # Step 2: entity_description (Update/Delete ORM statements)
    try:
        entity_description: dict[str, Any] | None = getattr(construct, "entity_description", None)
        if entity_description is not None:
            entity = entity_description.get("entity")
            if entity is not None and isinstance(entity, type) and hasattr(entity, "__table__"):
                models.add(entity)
    except Exception as entity_error:
        logger.warning("extract_models: entity_description parsing failed: %s", entity_error)

    # Step 3: CompoundSelect (union, intersect, except)
    selects: list[Any] | tuple[Any, ...] = getattr(construct, "selects", [])
    for sub_select in selects:
        _extract_from_construct(sub_select, models, visited)

    # Step 4: get_final_froms (select_from, joins -- catches aggregates like func.count())
    if hasattr(construct, "get_final_froms"):
        try:
            for from_clause in construct.get_final_froms():
                annotations: dict[str, Any] = getattr(from_clause, "_annotations", {})
                parent_mapper = annotations.get("parententity")
                if parent_mapper is not None and hasattr(parent_mapper, "class_"):
                    models.add(parent_mapper.class_)
        except Exception as froms_error:
            logger.warning("extract_models: get_final_froms parsing failed: %s", froms_error)

    # Step 5: WHERE criteria (subqueries, EXISTS, cross-table column references)
    where_criteria: tuple[Any, ...] = getattr(construct, "_where_criteria", ())
    for criterion in where_criteria:
        _extract_from_clause_tree(criterion, models, visited)


def _extract_from_clause_tree(clause: Any, models: set[type[Any]], visited: set[int]) -> None:
    """Walk a clause tree (WHERE criteria) looking for ORM model references."""
    clause_id = id(clause)
    if clause_id in visited:
        return
    visited.add(clause_id)

    # Check _annotations on columns (catches cross-table column references)
    annotations: dict[str, Any] = getattr(clause, "_annotations", {})
    parent_mapper = annotations.get("parententity")
    if parent_mapper is not None and hasattr(parent_mapper, "class_"):
        models.add(parent_mapper.class_)

    # Check for subqueries (scalar_subquery, exists, etc.)
    element = getattr(clause, "element", None)
    if element is not None and hasattr(element, "column_descriptions"):
        _extract_from_construct(element, models, visited)

    # Recurse into children
    try:
        for child in clause.get_children():
            _extract_from_clause_tree(child, models, visited)
    except Exception:
        pass


@dataclass
class Stats:
    inserted: int = 0
    archived: int = 0
    deleted: int = 0
    updated: int = 0
    number_of_records: int = 0


class BaseDBConnector(ABC):
    """Base database connector for all supported backends."""

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
        """Return host string as ip:port or server name based on available settings."""
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
        odbc_driver = self.settings.odbc_driver

        # WINDOWS AUTHENTICATION
        if self.auth_type == AuthMethods.WINDOWS:
            if self.settings.database_driver != DatabaseDriver.ODBC:
                raise ValueError("Windows Authentication requires lib pyodbc.")
            return f"mssql+pyodbc://@{host}/{self.database_name}?trusted_connection=yes&driver={odbc_driver}"
        # SQL USERNAME + PASSWORD AUTHENTICATION
        else:
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
    def __init__(
        self,
        settings: DatabaseSettings,
        app_id: str | None = None,
        schema_translate_map: dict[str | None, str | None] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a database interface with SQLAlchemy engine and optional object mapping.

        Args:
            settings: Configuration instance containing connection parameters.
            app_id: Hashed application identifier for dependency tracking. Required when
                settings.map_objects is True. Used to register database object dependencies
                (tables, views, routines) in AppDbObjects for analytics and orchestration.
            schema_translate_map: Optional schema name translation map applied to the engine.
                Redirects SQL schema references at the connection level without modifying ORM
                model definitions. Example: ``{None: "dc_example"}`` routes all tables with no
                explicit schema from the default schema (public/dbo) into ``dc_example``.
            **kwargs: Additional keyword arguments passed to SQLAlchemy's create_engine().
        """
        self.settings = settings
        self.settings_class = settings.__class__.__name__
        self.app_id: str | None = app_id
        self.logger: logging.Logger = logging.getLogger(__name__)
        self._system_db: Database | None = None
        self._schema_translate_map = schema_translate_map
        self.engine: Engine = self.engine_construct(**kwargs)
        if schema_translate_map is not None:
            self.engine = self.engine.execution_options(schema_translate_map=schema_translate_map)

    def engine_construct(self, **kwargs: Any) -> Engine:
        """Construct and return a SQLAlchemy Engine.

        Args:
            **kwargs: Additional keyword arguments passed to SQLAlchemy's create_engine().

        Returns:
            A SQLAlchemy Engine instance ready for connections.
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
        """Create a new SQLAlchemy session.

        Returns:
            SQLAlchemy session object. Use as context manager for automatic cleanup.
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

        self._register_object_models(obj_list)

        # Infer db_table from first ORM object if not explicitly provided
        db_table: type[Any] = obj_list[0].__class__

        # Fetch existing records from the database
        statement: Any = select(db_table)
        if filters is not None:
            statement = statement.filter(filters)
        db_data = self.query(statement, session, map_objects=False).scalars().all()

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
        """Delete a list of ORM objects from the database. Caller must commit afterward."""
        if not object_list:
            return

        for obj in object_list:
            session.delete(obj)

        self._register_object_models(object_list)

    def archive(
        self,
        object_list: list[Any],
        session: Session,
        archive_col: str = 'archive',
        archive_date: datetime | None = None,
    ) -> None:
        """Set archive timestamp on existing DB records. Caller must commit afterward."""
        if not object_list:
            return

        archive_time = archive_date or datetime.now(UTC)

        for obj in object_list:
            setattr(obj, archive_col, archive_time)
            session.add(obj)

        self._register_object_models(object_list)

    def bulk_insert(self, object_list: list[Any], session: Session) -> None:
        """Bulk insert ORM objects using session.add_all(). Caller must commit afterward."""
        if object_list:
            session.add_all(object_list)
            self._register_object_models(object_list)

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

        for obj in obj_list:
            filters = and_(*[(getattr(db_table, col) == getattr(obj, col)) for col in filter_cols])
            statement = select(db_table).where(filters)
            existing_records = self.query(statement, session).scalars().all()

            if not existing_records:
                self.add(obj, session)
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
                        self.add(record, session)
                        stats.updated += 1

        if commit:
            session.commit()

        return stats

    def query(
            self,
            statement: Select[Any],
            session: Session,
            *,
            map_objects: bool = True,
    ) -> Result[Any]:
        """Execute a SQLAlchemy 2.x select() statement with optional dependency tracking.

        Args:
            statement: A SQLAlchemy 2.x select() statement.
            session: Active SQLAlchemy session.
            map_objects: Override settings.map_objects for this query (default: True).

        Returns:
            Result object -- use .scalars().all() for ORM objects or .scalar_one_or_none() for single row.
        """
        if map_objects and self.settings.map_objects and self.app_id:
            self._extract_and_register_models(statement)

        return session.execute(statement)

    def run(
            self,
            statement: Executable,
            session: Session,
            *,
            models: set[type[Any]] | None = None,
    ) -> Result[Any]:
        """Execute a SQLAlchemy 2.x DML statement (update, delete, insert) with dependency tracking.

        Args:
            statement: A SQLAlchemy 2.x executable (update(), delete(), insert()).
            session: Active SQLAlchemy session.
            models: Explicit set of ORM model classes for dependency tracking.
                When None, models are auto-extracted from the statement.

        Returns:
            Result object -- use .rowcount for affected rows.
        """
        if self.settings.map_objects and self.app_id:
            self._extract_and_register_models(statement, explicit_models=models)

        return session.execute(statement)

    def add(
            self,
            instance: Any,
            session: Session,
            *,
            flush: bool = False,
    ) -> None:
        """Add a single ORM object to the session with dependency tracking.

        Args:
            instance: An ORM object to add to the session.
            session: Active SQLAlchemy session.
            flush: If True, calls session.flush() after add to obtain auto-generated values.
        """
        session.add(instance)

        if flush:
            session.flush()

        self._register_object_models([instance])

    def execute(self, sql_text: str, session: Session) -> Result[Any]:
        """Execute a stored procedure, function, or package procedure call.

        Only routine calls are allowed (CALL, EXEC, SELECT, BEGIN). The routine is
        validated against the database catalog and automatically registered in AppDbObjects.

        Args:
            sql_text: Raw SQL string containing a routine call.
            session: Active SQLAlchemy session.

        Returns:
            Result object from the executed routine.

        Raises:
            ValueError: If SQL is not a supported routine call or routine type is unknown.
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

    def _get_system_db(self) -> "Database":
        """Returns a cached Database instance connected to the main system database."""
        if self._system_db is None:
            self._system_db = Database(MainDatabaseSettings(), schema_translate_map=self._schema_translate_map)
        return self._system_db

    def _track_models_from_objects(self, objects: list[Any] | tuple[Any, ...]) -> set[type[Any]]:
        """Collect distinct ORM model classes from the given object list."""
        return {obj.__class__ for obj in objects if hasattr(obj, "__class__")}

    def _register_object_models(self, objects: list[Any] | tuple[Any, ...]) -> None:
        """Track and register ORM model classes from object instances for dependency mapping."""
        if self.settings.map_objects and self.app_id:
            models_used = self._track_models_from_objects(objects)
            self.register_models(models_used)

    def _extract_and_register_models(
            self,
            statement: Executable,
            explicit_models: set[type[Any]] | None = None,
    ) -> None:
        """Extract ORM models from a statement and register them for dependency tracking.

        Args:
            statement: A SQLAlchemy 2.x executable statement.
            explicit_models: If provided, skips extraction and registers these models directly.
        """
        detected_models = explicit_models
        if detected_models is None:
            try:
                detected_models = extract_models_from_statement(statement)
            except Exception as extraction_error:
                self.logger.warning("Failed to extract models from statement: %s", extraction_error)
                detected_models = set[type[Any]]()

        if detected_models:
            self.register_models(detected_models)
        elif not isinstance(statement, TextClause):
            self.logger.warning(_NO_MODELS_WARNING)

    def get_model_source_type(self, model: Any) -> str:
        """Identify whether a model is mapped to a table, view, function, or procedure."""
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

            return "unknown"

        except Exception as e:
            self.logger.warning("get_model_source_type failed for %s: %s", model.__name__, e)
            return "unknown"

    def get_routine_type(self, object_name: str, schema: str | None = None) -> str:
        """Return the type of a database routine by querying system catalogs.

        Queries information_schema.routines (PostgreSQL) or sys.objects (MSSQL)
        to determine whether the named object is a function or procedure.
        Raw SQL is used intentionally -- SQLAlchemy Inspector has no routine introspection API,
        and system catalog views are DBMS-owned objects that should not be ORM-mapped.

        Args:
            object_name: Name of the routine to look up.
            schema: Schema to search in. Defaults to 'public' (PostgreSQL).

        Returns:
            'FUNCTION', 'PROCEDURE', or 'unknown'.
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
        """Return the schema for a given model, with DBMS-specific fallbacks."""
        schema = db_object if isinstance(db_object, str) else db_object.__table__.schema

        if schema:
            return schema

        if self.settings.database_type == DatabaseType.POSTGRES:
            return "public"
        elif self.settings.database_type == DatabaseType.MSSQL:
            return "dbo"
        else:
            return "unknown"

    _VALID_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")

    def ensure_schema(self, schema_name: str) -> None:
        """Create a database schema if it does not already exist.

        Args:
            schema_name: Name of the schema to create.

        Raises:
            ValueError: If the schema name contains invalid characters.
        """
        if not self._VALID_SCHEMA_NAME.match(schema_name):
            raise ValueError(f"Invalid schema name: {schema_name!r}")
        with self.engine.connect() as conn:
            if self.settings.database_type == DatabaseType.POSTGRES:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            elif self.settings.database_type == DatabaseType.MSSQL:
                conn.execute(text(
                    f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}') "
                    f"EXEC('CREATE SCHEMA [{schema_name}]')"
                ))
            else:
                raise NotImplementedError(f"ensure_schema not implemented for {self.settings.database_type}")
            conn.commit()

    def extract_database_and_schema(self, db_object: Any) -> tuple[str | None, str]:
        """Extract (database_name, schema_name) from a model's schema definition.

        Handles cross-database formats like 'OtherDB..' in MSSQL, where '..' implies
        the default schema (dbo).
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
        """Register ORM model classes as application dependencies in AppDbObjects."""
        system_db = self._get_system_db()
        with system_db.create_session() as session:
            dependencies: list[Any] = []
            dependencies_hash: list[str] = []

            for db_object in db_objects:
                try:
                    object_name = db_object.__table__.name
                    db_name_override, schema = self.extract_database_and_schema(db_object)
                    object_type = self.get_model_source_type(db_object)
                    database_name = db_name_override or self.settings.database_name or ""

                    record_data = self.prepare_dependency_record(
                        object_name=object_name,
                        object_type=object_type,
                        schema=schema,
                        database_name=database_name,
                    )
                    record_hash = cast(str, record_data["sha"])
                    if record_hash not in dependencies_hash:
                        dependency = AppDbObjects(**record_data)
                        dependencies.append(dependency)
                        dependencies_hash.append(record_hash)

                except Exception as e:
                    self.logger.error("[register_models] Failed to register %s: %s", db_object.__name__, e)

            if dependencies:
                filters = and_(AppDbObjects.app_id == self.app_id)
                system_db.merge(dependencies, session, filters=filters)


    def register_sql_objects(self, objects: list[dict[str, Any]]) -> None:
        """Register non-ORM database objects (from raw SQL) as application dependencies."""
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
                    self.logger.error("[register_sql_objects] Failed to register %s: %s", obj, e)

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
        """Prepare a fully hashed and timestamped dictionary for AppDbObjects registration."""
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
        """Compute the SHA value based on defined keys and fields."""
        return cast(str, runtime.make_hash(self.get_fields(), on_keys=self.get_hash_keys()))

    def __init__(self, **kwargs: Any) -> None:
        auto_sha: bool = kwargs.pop("auto_sha", True)
        super().__init__(**kwargs)
        if auto_sha and not getattr(self, "sha", None):
            self.sha = self.compute_sha()
