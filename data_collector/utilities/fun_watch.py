"""@fun_watch decorator for function-level performance monitoring.

Provides automatic function registration (AppFunctions) and per-invocation
metric recording (FunctionLog) with thread-safe context-local counters.
"""

from __future__ import annotations

import functools
import inspect
import logging
import threading
from collections.abc import Callable
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any, TypeVar

from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.apps import AppFunctions
from data_collector.tables.log import FunctionLog
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.functions.runtime import make_hash

logger = logging.getLogger(__name__)
T = TypeVar("T")


# ---------------------------------------------------------------------------
# FunWatchContext
# ---------------------------------------------------------------------------

class FunWatchContext:
    """Per-invocation counters used by the ``@fun_watch`` wrapper.

    Application code updates the active invocation context through
    ``self._fw_ctx.mark_solved()`` / ``self._fw_ctx.mark_failed()``.
    The active context is bound using context-local state.
    """

    __slots__ = ("solved", "failed", "task_size", "_counter_lock")

    def __init__(self, task_size: int | None = None) -> None:
        self.solved: int = 0
        self.failed: int = 0
        self.task_size: int | None = task_size
        self._counter_lock = threading.Lock()

    def mark_solved(self, count: int = 1) -> None:
        """Increment the solved counter."""
        with self._counter_lock:
            self.solved += count

    def mark_failed(self, count: int = 1) -> None:
        """Increment the failed counter."""
        with self._counter_lock:
            self.failed += count

    def snapshot(self) -> tuple[int, int]:
        """Return an atomic snapshot of solved and failed counters."""
        with self._counter_lock:
            return self.solved, self.failed


class _FunWatchContextProxy:
    """Proxy exposed as ``self._fw_ctx`` that resolves active context-local state."""

    __slots__ = ("_registry",)

    def __init__(self, registry: FunWatchRegistry) -> None:
        self._registry = registry

    def mark_solved(self, count: int = 1) -> None:
        """Forward solved counter updates to the active invocation context."""
        self._registry.get_active_context().mark_solved(count)

    def mark_failed(self, count: int = 1) -> None:
        """Forward failed counter updates to the active invocation context."""
        self._registry.get_active_context().mark_failed(count)

    def __getattr__(self, name: str) -> Any:
        """Forward attribute access to the active invocation context."""
        return getattr(self._registry.get_active_context(), name)


# ---------------------------------------------------------------------------
# FunWatchRegistry — singleton that owns all decorator state
# ---------------------------------------------------------------------------

class FunWatchRegistry:
    """Thread-safe singleton that manages function registration, counters, and DB access.

    All mutable state lives here instead of in module-level globals.
    Access the singleton via ``FunWatchRegistry.instance()``.
    """

    _instance: FunWatchRegistry | None = None
    _instance_lock: threading.Lock = threading.Lock()

    def __init__(self) -> None:
        self._registered_functions: dict[str, bool] = {}
        self._registration_lock = threading.Lock()

        self._function_counters: dict[tuple[str, int], int] = {}
        self._counter_lock = threading.Lock()

        self._system_db: Database | None = None
        self._db_lock = threading.Lock()
        self._active_context: ContextVar[FunWatchContext | None] = ContextVar("fun_watch_active_context", default=None)
        self._context_proxy = _FunWatchContextProxy(self)

    @classmethod
    def instance(cls) -> FunWatchRegistry:
        """Return the process-wide singleton, creating it on first call."""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Discard the singleton and all cached state.  Intended for tests only."""
        with cls._instance_lock:
            cls._instance = None

    # -- Database access ----------------------------------------------------

    def _get_system_db(self) -> Database:
        """Return a lazily-created Database for system table writes."""
        if self._system_db is None:
            with self._db_lock:
                if self._system_db is None:
                    self._system_db = Database(MainDatabaseSettings())
        return self._system_db

    # -- Function counters --------------------------------------------------

    def next_execution_order(self, runtime_id: str, thread_id: int) -> int:
        """Return and increment the per-(runtime, thread) execution ordinal."""
        key = (runtime_id, thread_id)
        with self._counter_lock:
            current = self._function_counters.get(key, 0) + 1
            self._function_counters[key] = current
        return current

    # -- Invocation context --------------------------------------------------

    def bind_context(self, ctx: FunWatchContext) -> Token[FunWatchContext | None]:
        """Bind invocation context for the current execution flow."""
        return self._active_context.set(ctx)

    def unbind_context(self, token: Token[FunWatchContext | None]) -> None:
        """Restore the previous invocation context for the current execution flow."""
        self._active_context.reset(token)

    def get_active_context(self) -> FunWatchContext:
        """Return current invocation context or raise if none is bound."""
        ctx = self._active_context.get()
        if ctx is None:
            raise RuntimeError(
                "FunWatchContext is not active. "
                "Use self._fw_ctx only inside methods decorated with @fun_watch, "
                "or wrap worker callables with FunWatchRegistry.wrap_with_active_context()."
            )
        return ctx

    def wrap_with_active_context(self, func: Callable[..., T]) -> Callable[..., T]:
        """Wrap callable so worker threads inherit active @fun_watch context."""
        parent_ctx = self.get_active_context()

        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> T:
            token = self.bind_context(parent_ctx)
            try:
                return func(*args, **kwargs)
            finally:
                self.unbind_context(token)

        return wrapped

    def ensure_context_proxy(self, instance: Any) -> None:
        """Attach the stable context proxy to instance as ``_fw_ctx``."""
        if getattr(instance, "_fw_ctx", None) is not self._context_proxy:
            instance._fw_ctx = self._context_proxy

    # -- AppFunctions registration ------------------------------------------

    def register_function(
        self,
        function_hash: str,
        function_name: str,
        filepath: str,
        app_id: str,
    ) -> None:
        """Upsert an AppFunctions row and cache the hash (double-checked locking)."""
        if function_hash in self._registered_functions:
            return

        with self._registration_lock:
            if function_hash in self._registered_functions:
                return

            now = datetime.now(UTC)
            db = self._get_system_db()
            try:
                with db.create_session() as session:
                    record = AppFunctions(
                        function_hash=function_hash,
                        function_name=function_name,
                        filepath=filepath,
                        app_id=app_id,
                        first_seen=now,
                        last_seen=now,
                    )
                    db.update_insert(
                        record,
                        session,
                        filter_cols=["function_hash"],
                    )
                self._registered_functions[function_hash] = True
            except Exception:
                logger.exception("Failed to register function %s", function_name)

    # -- AppFunctions.last_seen update --------------------------------------

    def update_last_seen(self, function_hash: str) -> None:
        """Touch AppFunctions.last_seen for the given hash."""
        db = self._get_system_db()
        try:
            with db.create_session() as session:
                record = (
                    session.query(AppFunctions)
                    .filter(AppFunctions.function_hash == function_hash)
                    .first()
                )
                if record is not None:
                    record.last_seen = datetime.now(UTC)  # type: ignore[assignment]
                    session.commit()
        except Exception:
            logger.exception("Failed to update last_seen for %s", function_hash)

    # -- FunctionLog insertion ----------------------------------------------

    def insert_function_log(
        self,
        *,
        function_hash: str,
        execution_order: int,
        main_app: str,
        app_id: str,
        thread_id: int,
        task_size: int | None,
        solved: int,
        failed: int,
        start_time: datetime,
        end_time: datetime,
        runtime_id: str,
    ) -> None:
        """Insert a single FunctionLog row."""
        db = self._get_system_db()
        try:
            record = FunctionLog(
                function_hash=function_hash,
                execution_order=execution_order,
                main_app=main_app,
                app_id=app_id,
                thread_id=thread_id,
                task_size=task_size,
                solved=solved,
                failed=failed,
                start_time=start_time,
                end_time=end_time,
                totals=get_totals(start_time, end_time),
                totalm=get_totalm(start_time, end_time),
                totalh=get_totalh(start_time, end_time),
                runtime=runtime_id,
            )
            with db.create_session() as session:
                session.add(record)
                session.commit()
        except Exception:
            logger.exception("Failed to insert FunctionLog for %s", function_hash)


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def fun_watch(func: Any) -> Any:
    """Decorator that monitors function execution and records metrics to DB.

    The decorated method's instance (``self``) must expose:

    * ``self.app_id: str``  -- application hash identifier
    * ``self.runtime: str``  -- current runtime hash

    Optionally:

    * ``self.main_app: str``  -- root app hash (defaults to ``self.app_id``)

    During execution the wrapper binds a per-invocation :class:`FunWatchContext`
    in context-local state. The instance receives a stable ``self._fw_ctx`` proxy
    that routes ``mark_solved`` / ``mark_failed`` to the active context.
    """

    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        registry = FunWatchRegistry.instance()

        # --- Extract required attributes ---
        app_id: str = getattr(self, "app_id", "")
        runtime_id: str = getattr(self, "runtime", "")
        main_app: str = getattr(self, "main_app", "") or app_id

        if not app_id or not runtime_id:
            raise TypeError(
                f"@fun_watch requires 'app_id' and 'runtime' attributes on the instance. "
                f"Got app_id={app_id!r}, runtime={runtime_id!r}"
            )

        # --- Function identity ---
        function_name = func.__name__
        filepath = inspect.getfile(func)
        function_hash_value: str = str(make_hash(app_id + function_name))

        # --- Step 1: Auto-register (cached) ---
        registry.register_function(function_hash_value, function_name, filepath, app_id)

        # --- Step 2: Determine task_size ---
        task_size: int | None = None
        if args and hasattr(args[0], "__len__"):
            task_size = len(args[0])

        # --- Step 3: Bind per-invocation context ---
        ctx = FunWatchContext(task_size=task_size)
        registry.ensure_context_proxy(self)
        context_token = registry.bind_context(ctx)

        # --- Step 4: Execute with timing ---
        thread_id = threading.get_ident()
        execution_order = registry.next_execution_order(runtime_id, thread_id)
        start_time = datetime.now(UTC)

        try:
            result = func(self, *args, **kwargs)
        finally:
            try:
                solved, failed = ctx.snapshot()
                end_time = datetime.now(UTC)
                registry.insert_function_log(
                    function_hash=function_hash_value,
                    execution_order=execution_order,
                    main_app=main_app,
                    app_id=app_id,
                    thread_id=thread_id,
                    task_size=ctx.task_size,
                    solved=solved,
                    failed=failed,
                    start_time=start_time,
                    end_time=end_time,
                    runtime_id=runtime_id,
                )
                registry.update_last_seen(function_hash_value)
            finally:
                registry.unbind_context(context_token)

        return result

    return wrapper
