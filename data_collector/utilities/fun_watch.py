"""@fun_watch decorator for function-level performance monitoring.

Provides automatic function registration (AppFunctions) and per-invocation
metric recording (FunctionLog) with thread-safe context-local counters.
Binds ``function_id`` into structlog context for log correlation.
"""

from __future__ import annotations

import functools
import inspect
import json
import logging
import sys
import threading
from collections.abc import Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import structlog  # type: ignore[import-untyped]
from sqlalchemy import select

from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.apps import AppFunctions
from data_collector.tables.log import FunctionLog, FunctionLogError
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.functions.runtime import make_hash

logger = structlog.get_logger(__name__)
T = TypeVar("T")

_INTERNAL_MODULE_PREFIXES: tuple[str, ...] = (
    "logging",
    "structlog",
    "queue",
    "threading",
    "concurrent.futures",
    "concurrent",
    "data_collector.utilities.log",
    "data_collector.utilities.fun_watch",
    "runpy",
    "importlib",
    "_frozen_importlib",
)


def _find_root_caller() -> str | None:
    """Walk the call stack to find the first non-internal, non-synthetic caller.

    Used to anchor the call_chain root (e.g. ``main``) when no parent
    ``@fun_watch`` context exists.  Returns ``None`` when all frames above the
    wrapper are internal (typical for ``ThreadPoolExecutor`` worker threads).
    """
    frame = inspect.currentframe()
    if frame is None:
        return None
    try:
        frame = frame.f_back  # skip _find_root_caller itself
        while frame is not None:
            module = frame.f_globals.get("__name__", "")
            if not any(module.startswith(p) for p in _INTERNAL_MODULE_PREFIXES):
                fn_name = frame.f_code.co_name
                if not fn_name.startswith("<"):
                    return fn_name
            frame = frame.f_back
    finally:
        del frame
    return None


def _get_class_lineno(cls: type, fallback: int) -> int:
    """Return the source line number of a class definition, or fallback."""
    try:
        _, lineno = inspect.findsource(cls)
        return lineno
    except (OSError, TypeError):
        return fallback


class FunWatchContext:
    """Per-invocation counters used by the ``@fun_watch`` wrapper.

    Application code updates the active invocation context through
    ``self._fun_watch.mark_solved()`` / ``self._fun_watch.mark_failed()``.
    The active context is bound using context-local state.
    """

    _MAX_ERROR_SAMPLES: int = 5

    __slots__ = ("solved", "failed", "task_size", "log_id", "_counter_lock", "_error_types", "_error_samples")

    def __init__(self, task_size: int | None = None) -> None:
        self.solved: int = 0
        self.failed: int = 0
        self.task_size: int | None = task_size
        self.log_id: int | None = None
        self._counter_lock = threading.Lock()
        self._error_types: dict[str, int] = {}
        self._error_samples: dict[str, list[str]] = {}

    def mark_solved(self, count: int = 1) -> None:
        """Increment the solved counter."""
        with self._counter_lock:
            self.solved += count

    def mark_failed(
        self,
        count: int = 1,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Increment the failed counter with optional error detail aggregation."""
        with self._counter_lock:
            self.failed += count
            if error_type is not None:
                self._error_types[error_type] = self._error_types.get(error_type, 0) + count
                if error_message is not None:
                    samples = self._error_samples.setdefault(error_type, [])
                    if len(samples) < self._MAX_ERROR_SAMPLES:
                        samples.append(error_message)

    def snapshot(self) -> tuple[int, int]:
        """Return an atomic snapshot of solved and failed counters."""
        with self._counter_lock:
            return self.solved, self.failed

    def error_snapshot(self) -> tuple[int, str | None, str | None]:
        """Return an atomic snapshot of item error details.

        Returns:
            Tuple of (item_error_count, item_error_types_json, item_error_samples_json).
            JSON strings are None when no typed errors were recorded.
        """
        with self._counter_lock:
            if not self._error_types:
                return 0, None, None
            item_error_count = sum(self._error_types.values())
            types_json = json.dumps(self._error_types, separators=(",", ":"))
            samples_json = json.dumps(self._error_samples, separators=(",", ":")) if self._error_samples else None
            return item_error_count, types_json, samples_json


class _FunWatchContextProxy:
    """Proxy exposed as ``self._fun_watch`` that resolves active context-local state."""

    __slots__ = ("_registry",)

    def __init__(self, registry: FunWatchRegistry) -> None:
        self._registry = registry

    def mark_solved(self, count: int = 1) -> None:
        """Forward solved counter updates to the active invocation context."""
        self._registry.get_active_context().mark_solved(count)

    def mark_failed(
        self,
        count: int = 1,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Forward failed counter updates to the active invocation context."""
        self._registry.get_active_context().mark_failed(count, error_type=error_type, error_message=error_message)

    def set_task_size(self, size: int) -> None:
        """Set the task_size on the active invocation context."""
        self._registry.get_active_context().task_size = size

    def __getattr__(self, name: str) -> Any:
        """Forward attribute access to the active invocation context."""
        return getattr(self._registry.get_active_context(), name)


class FunWatchMixin:
    """Mixin that declares the ``_fun_watch`` attribute for type-safe access.

    Classes using ``@fun_watch`` should inherit from this mixin so that
    ``self._fun_watch.mark_solved()`` / ``self._fun_watch.mark_failed()``
    pass pyright strict mode without ``# type: ignore`` suppressions.
    """

    _fun_watch: _FunWatchContextProxy


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

        self._global_counters: dict[str, int] = {}
        self._thread_counters: dict[tuple[str, int], int] = {}
        self._counter_lock = threading.Lock()
        self._current_runtime: str | None = None

        self._default_lifecycle_log_level: int = logging.DEBUG

        self._system_db: Database | None = None
        self._db_lock = threading.Lock()
        self._active_context: ContextVar[FunWatchContext | None] = ContextVar(
            "fun_watch_active_context", default=None
        )
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

    def _get_system_db(self) -> Database:
        """Return a lazily-created Database for system table writes."""
        if self._system_db is None:
            with self._db_lock:
                if self._system_db is None:
                    self._system_db = Database(MainDatabaseSettings())
        return self._system_db

    @property
    def default_lifecycle_log_level(self) -> int:
        """Return the default log level for @fun_watch lifecycle messages."""
        return self._default_lifecycle_log_level

    def set_default_lifecycle_log_level(self, level: int) -> None:
        """Set the default log level for @fun_watch lifecycle messages.

        Typically called from app main() after creating LogSettings:
        ``FunWatchRegistry.instance().set_default_lifecycle_log_level(log_settings.log_level)``
        """
        self._default_lifecycle_log_level = level

    def next_execution_order(self, runtime_id: str, thread_id: int) -> tuple[int, int]:
        """Return and increment (global_order, thread_order) execution ordinals.

        global_order increments across all threads (chronological).
        thread_order increments independently per thread.

        Clears stale counters when a new runtime_id is detected to prevent
        unbounded memory growth in long-running processes.
        """
        thread_key = (runtime_id, thread_id)
        with self._counter_lock:
            if self._current_runtime is not None and self._current_runtime != runtime_id:
                self._global_counters.clear()
                self._thread_counters.clear()
            self._current_runtime = runtime_id
            global_order = self._global_counters.get(runtime_id, 0) + 1
            self._global_counters[runtime_id] = global_order
            thread_order = self._thread_counters.get(thread_key, 0) + 1
            self._thread_counters[thread_key] = thread_order
        return global_order, thread_order

    def bind_context(self, context: FunWatchContext) -> Token[FunWatchContext | None]:
        """Bind invocation context for the current execution flow."""
        return self._active_context.set(context)

    def unbind_context(self, token: Token[FunWatchContext | None]) -> None:
        """Restore the previous invocation context for the current execution flow."""
        self._active_context.reset(token)

    def get_active_context(self) -> FunWatchContext:
        """Return current invocation context or raise if none is bound."""
        context = self._active_context.get()
        if context is None:
            raise RuntimeError(
                "FunWatchContext is not active. "
                "Use self._fun_watch only inside methods decorated with @fun_watch, "
                "or wrap worker callables with FunWatchRegistry.wrap_with_active_context()."
            )
        return context

    def wrap_with_active_context(self, func: Callable[..., T]) -> Callable[..., T]:
        """Wrap callable so worker threads inherit active @fun_watch context.

        Propagates both the FunWatchContext (for solved/failed counters) and
        structlog contextvars (for function_id, call_chain, thread_id in logs).
        """
        parent_context = self.get_active_context()
        parent_structlog_context = structlog.contextvars.get_contextvars()

        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> T:
            token = self.bind_context(parent_context)
            structlog.contextvars.bind_contextvars(**parent_structlog_context)
            structlog.contextvars.bind_contextvars(thread_id=threading.get_ident())
            try:
                return func(*args, **kwargs)
            finally:
                structlog.contextvars.unbind_contextvars(*parent_structlog_context.keys())
                self.unbind_context(token)

        return wrapped

    def try_get_active_context(self) -> FunWatchContext | None:
        """Return the currently active context, or None if none is bound."""
        return self._active_context.get()

    def get_parent_log_id(self) -> int | None:
        """Return the log_id of the currently active context, or None."""
        context = self._active_context.get()
        return context.log_id if context is not None else None

    def ensure_context_proxy(self, instance: Any) -> None:
        """Attach the stable context proxy to instance as ``_fun_watch``."""
        if getattr(instance, "_fun_watch", None) is not self._context_proxy:
            instance._fun_watch = self._context_proxy

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
                logger.exception("Failed to register function", function_name=function_name)

    def update_last_seen(self, function_hash: str) -> None:
        """Touch AppFunctions.last_seen for the given hash."""
        db = self._get_system_db()
        try:
            with db.create_session() as session:
                stmt = select(AppFunctions).where(AppFunctions.function_hash == function_hash)
                record = db.query(stmt, session).scalar_one_or_none()
                if record is not None:
                    record.last_seen = datetime.now(UTC)  # type: ignore[assignment]
                    session.commit()
        except Exception:
            logger.exception("Failed to update last_seen", function_hash=function_hash)

    def start_function_log(
        self,
        *,
        function_hash: str,
        execution_order: int,
        thread_execution_order: int,
        main_app: str,
        app_id: str,
        thread_id: int,
        task_size: int | None,
        start_time: datetime,
        runtime_id: str,
        parent_log_id: int | None,
        log_role: str,
    ) -> int | None:
        """INSERT a partial FunctionLog row and return its auto-generated id."""
        db = self._get_system_db()
        try:
            record = FunctionLog(
                function_hash=function_hash,
                execution_order=execution_order,
                thread_execution_order=thread_execution_order,
                main_app=main_app,
                app_id=app_id,
                thread_id=thread_id,
                task_size=task_size,
                start_time=start_time,
                runtime=runtime_id,
                parent_log_id=parent_log_id,
                log_role=log_role,
            )
            with db.create_session() as session:
                db.add(record, session, flush=True)
                log_id: int = record.id  # type: ignore[assignment]
                session.commit()
            return log_id
        except Exception:
            logger.exception("Failed to start FunctionLog", function_hash=function_hash)
            return None

    def complete_function_log(
        self,
        *,
        log_id: int | None,
        solved: int,
        failed: int,
        start_time: datetime,
        end_time: datetime,
        exc_occurred: bool = False,
        error_type: str | None = None,
        error_message: str | None = None,
        item_error_count: int = 0,
        item_error_types_json: str | None = None,
        item_error_samples_json: str | None = None,
        task_size: int | None = None,
    ) -> None:
        """UPDATE an existing FunctionLog row with final metrics and error details."""
        if log_id is None:
            return
        db = self._get_system_db()
        try:
            with db.create_session() as session:
                record = session.get(FunctionLog, log_id)
                if record is not None:
                    record.solved = solved  # type: ignore[assignment]
                    record.failed = failed  # type: ignore[assignment]
                    record.processed_count = solved + failed  # type: ignore[assignment]
                    record.is_success = not exc_occurred and failed == 0  # type: ignore[assignment]
                    record.end_time = end_time  # type: ignore[assignment]
                    record.totals = get_totals(start_time, end_time)  # type: ignore[assignment]
                    record.totalm = get_totalm(start_time, end_time)  # type: ignore[assignment]
                    record.totalh = get_totalh(start_time, end_time)  # type: ignore[assignment]
                    if task_size is not None:
                        record.task_size = task_size  # type: ignore[assignment]
                    if error_type is not None or item_error_count > 0:
                        error_record = FunctionLogError(
                            function_log_id=log_id,
                            error_type=error_type,
                            error_message=error_message,
                            item_error_count=item_error_count,
                            item_error_types_json=item_error_types_json,
                            item_error_samples_json=item_error_samples_json,
                        )
                        db.add(error_record, session)
                    session.commit()
        except Exception:
            logger.exception("Failed to complete FunctionLog", log_id=log_id)

    def update_parent_log_role(self, log_id: int | None) -> None:
        """Update a FunctionLog row's log_role to 'parent'."""
        if log_id is None:
            return
        db = self._get_system_db()
        try:
            with db.create_session() as session:
                record = session.get(FunctionLog, log_id)
                if record is not None and str(record.log_role) != "parent":
                    record.log_role = "parent"  # type: ignore[assignment]
                    session.commit()
        except Exception:
            logger.exception("Failed to update parent log_role", log_id=log_id)


def _finalize_fun_watch(
    *,
    invocation_context: FunWatchContext,
    registry: FunWatchRegistry,
    start_time: datetime,
    exc_occurred: bool,
    caught_error_type: str | None,
    caught_error_message: str | None,
    log_lifecycle: bool,
    effective_log_level: int,
    app_logger: Any,
    function_name: str,
    function_id: str,
    app_id: str,
    runtime_id: str,
    caller_module_name: str,
    filepath: str,
    func_lineno: int,
    current_call_chain: str,
    prev_function_id: Any,
    prev_call_chain: Any,
    prev_thread_id: Any,
    context_token: Token[FunWatchContext | None],
) -> None:
    """Shared finalization logic for sync and async @fun_watch wrappers."""
    try:
        solved, failed = invocation_context.snapshot()
        item_error_count, item_error_types_json, item_error_samples_json = (
            invocation_context.error_snapshot()
        )
        end_time = datetime.now(UTC)
        duration_s = (end_time - start_time).total_seconds()
        registry.complete_function_log(
            log_id=invocation_context.log_id,
            solved=solved,
            failed=failed,
            start_time=start_time,
            end_time=end_time,
            exc_occurred=exc_occurred,
            error_type=caught_error_type,
            error_message=caught_error_message,
            item_error_count=item_error_count,
            item_error_types_json=item_error_types_json,
            item_error_samples_json=item_error_samples_json,
            task_size=invocation_context.task_size,
        )
        registry.update_last_seen(function_id)
        if not exc_occurred and log_lifecycle:
            app_logger.log(
                effective_log_level,
                "Function completed",
                function_name=function_name,
                function_id=function_id,
                app_id=app_id,
                runtime=runtime_id,
                solved=solved,
                failed=failed,
                processed_count=solved + failed,
                is_success=failed == 0,
                task_size=invocation_context.task_size,
                duration_s=duration_s,
                log_id=invocation_context.log_id,
                module_name=caller_module_name,
                module_path=filepath,
                lineno=func_lineno,
                call_chain=current_call_chain,
            )
    finally:
        if prev_function_id is not None:
            structlog.contextvars.bind_contextvars(function_id=prev_function_id)
        else:
            structlog.contextvars.unbind_contextvars("function_id")
        if prev_call_chain is not None:
            structlog.contextvars.bind_contextvars(call_chain=prev_call_chain)
        else:
            structlog.contextvars.unbind_contextvars("call_chain")
        if prev_thread_id is not None:
            structlog.contextvars.bind_contextvars(thread_id=prev_thread_id)
        else:
            structlog.contextvars.unbind_contextvars("thread_id")
        registry.unbind_context(context_token)


@dataclass
class _FunWatchSetupResult:
    """Holds all computed state from the shared setup phase of @fun_watch."""

    registry: FunWatchRegistry
    invocation_context: FunWatchContext
    context_token: Token[FunWatchContext | None]
    start_time: datetime
    function_name: str = ""
    function_id: str = ""
    app_id: str = ""
    runtime_id: str = ""
    caller_module_name: str = ""
    filepath: str = ""
    func_lineno: int = 0
    current_call_chain: str = ""
    effective_log_level: int = 0
    app_logger: Any = field(default=None)
    prev_function_id: Any = None
    prev_call_chain: Any = None
    prev_thread_id: Any = None


def _setup_fun_watch_invocation(
    decorated_func: Any,
    instance: Any,
    args: tuple[Any, ...],
    *,
    task_size_detect: bool,
    log_lifecycle: bool,
    log_level: int | None,
) -> _FunWatchSetupResult:
    """Shared setup logic for sync and async @fun_watch wrappers.

    Performs function registration, context binding, structlog context
    management, and lifecycle logging. Returns all state needed by the
    caller to invoke the decorated function and finalize.
    """
    registry = FunWatchRegistry.instance()

    app_id: str = getattr(instance, "app_id", "")
    runtime_id: str = getattr(instance, "runtime", "")
    main_app: str = getattr(instance, "main_app", "") or app_id

    if not app_id or not runtime_id:
        raise TypeError(
            f"@fun_watch requires 'app_id' and 'runtime' attributes on the instance. "
            f"Got app_id={app_id!r}, runtime={runtime_id!r}"
        )

    function_name = decorated_func.__name__
    class_name = type(instance).__name__
    chain_label = f"{class_name}.{function_name}"
    definition_filepath = inspect.getfile(decorated_func)
    func_lineno = decorated_func.__code__.co_firstlineno

    func_defining_module = getattr(decorated_func, "__module__", None)
    instance_class: type = type(instance)  # pyright: ignore[reportUnknownVariableType]
    instance_module_name: str = instance_class.__module__
    if func_defining_module and func_defining_module != instance_module_name:
        instance_module = sys.modules.get(instance_module_name)
        instance_file = getattr(instance_module, "__file__", None) if instance_module else None
        if isinstance(instance_file, str):
            filepath = instance_file
            func_lineno = _get_class_lineno(instance_class, func_lineno)
        else:
            filepath = definition_filepath
    else:
        filepath = definition_filepath

    caller_module_name = Path(filepath).name
    function_id: str = str(make_hash(app_id + function_name))

    registry.register_function(function_id, function_name, definition_filepath, app_id)

    detected_task_size: int | None = None
    if task_size_detect and args and hasattr(args[0], "__len__"):
        detected_task_size = len(args[0])

    invocation_context = FunWatchContext(task_size=detected_task_size)
    registry.ensure_context_proxy(instance)

    parent_log_id = registry.get_parent_log_id()
    log_role = "child" if parent_log_id is not None else "single"

    context_token = registry.bind_context(invocation_context)

    previous_structlog_context = structlog.contextvars.get_contextvars()
    prev_function_id = previous_structlog_context.get("function_id")
    prev_call_chain = previous_structlog_context.get("call_chain")
    prev_thread_id = previous_structlog_context.get("thread_id")
    if prev_call_chain:
        current_call_chain = f"{prev_call_chain} -> {chain_label}"
    else:
        root_caller = _find_root_caller()
        if root_caller and root_caller != function_name:
            current_call_chain = f"{root_caller} -> {chain_label}"
        else:
            current_call_chain = chain_label
    thread_id = threading.get_ident()
    structlog.contextvars.bind_contextvars(
        function_id=function_id, call_chain=current_call_chain, thread_id=thread_id,
    )
    execution_order, thread_execution_order = registry.next_execution_order(runtime_id, thread_id)
    start_time = datetime.now(UTC)

    invocation_context.log_id = registry.start_function_log(
        function_hash=function_id,
        execution_order=execution_order,
        thread_execution_order=thread_execution_order,
        main_app=main_app,
        app_id=app_id,
        thread_id=thread_id,
        task_size=invocation_context.task_size,
        start_time=start_time,
        runtime_id=runtime_id,
        parent_log_id=parent_log_id,
        log_role=log_role,
    )
    if parent_log_id is not None:
        registry.update_parent_log_role(parent_log_id)

    effective_log_level = log_level if log_level is not None else registry.default_lifecycle_log_level
    app_logger: Any = getattr(instance, "logger", logger)
    if log_lifecycle:
        lifecycle_started_extras: dict[str, Any] = {}
        if detected_task_size is not None:
            lifecycle_started_extras["task_size"] = detected_task_size
        app_logger.log(
            effective_log_level,
            "Function started",
            function_name=function_name,
            function_id=function_id,
            app_id=app_id,
            runtime=runtime_id,
            log_id=invocation_context.log_id,
            execution_order=execution_order,
            log_role=log_role,
            module_name=caller_module_name,
            module_path=filepath,
            lineno=func_lineno,
            call_chain=current_call_chain,
            **lifecycle_started_extras,
        )

    return _FunWatchSetupResult(
        registry=registry,
        invocation_context=invocation_context,
        context_token=context_token,
        start_time=start_time,
        function_name=function_name,
        function_id=function_id,
        app_id=app_id,
        runtime_id=runtime_id,
        caller_module_name=caller_module_name,
        filepath=filepath,
        func_lineno=func_lineno,
        current_call_chain=current_call_chain,
        effective_log_level=effective_log_level,
        app_logger=app_logger,
        prev_function_id=prev_function_id,
        prev_call_chain=prev_call_chain,
        prev_thread_id=prev_thread_id,
    )


def fun_watch(
    func: Any = None,
    *,
    task_size: bool = True,
    log_lifecycle: bool = True,
    log_level: int | None = None,
) -> Any:
    """Decorator that monitors function execution and records metrics to DB.

    Supports both bare ``@fun_watch`` and parameterized ``@fun_watch(...)`` usage.

    Args:
        task_size: Auto-detect task_size from first list-like argument (default True).
        log_lifecycle: Emit "Function started"/"Function completed" lifecycle logs
            (default True). FunctionLog row is always written regardless.
        log_level: Log level for lifecycle messages. ``None`` uses the registry
            default (from LogSettings.log_level). Explicit value overrides.

    The decorated method's instance (``self``) must expose:

    * ``self.app_id: str``  -- application hash identifier
    * ``self.runtime: str``  -- current runtime hash

    Optionally:

    * ``self.main_app: str``  -- root app hash (defaults to ``self.app_id``)
    * ``self.logger``  -- structlog BoundLogger from ``LoggingService.configure_logger()``.
      When present, lifecycle logs flow through the application's logging pipeline
      (DB, Splunk, console). When absent, falls back to the module-level logger (stderr only).
    """

    def decorator(decorated_func: Any) -> Any:
        @functools.wraps(decorated_func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            setup = _setup_fun_watch_invocation(
                decorated_func, self, args,
                task_size_detect=task_size, log_lifecycle=log_lifecycle, log_level=log_level,
            )

            exc_occurred = False
            caught_error_type: str | None = None
            caught_error_message: str | None = None
            try:
                result = decorated_func(self, *args, **kwargs)
            except Exception as exc:
                exc_occurred = True
                caught_error_type = type(exc).__name__
                caught_error_message = str(exc)
                _solved, _failed = setup.invocation_context.snapshot()
                setup.app_logger.exception(
                    "Unhandled exception in @fun_watch decorated function",
                    function_name=setup.function_name,
                    function_id=setup.function_id,
                    app_id=setup.app_id,
                    runtime=setup.runtime_id,
                    error_type=caught_error_type,
                    error_message=caught_error_message,
                    solved=_solved,
                    failed=_failed,
                    is_success=False,
                    log_id=setup.invocation_context.log_id,
                    module_name=setup.caller_module_name,
                    module_path=setup.filepath,
                    lineno=setup.func_lineno,
                    call_chain=setup.current_call_chain,
                )
                raise
            finally:
                _finalize_fun_watch(
                    invocation_context=setup.invocation_context,
                    registry=setup.registry,
                    start_time=setup.start_time,
                    exc_occurred=exc_occurred,
                    caught_error_type=caught_error_type,
                    caught_error_message=caught_error_message,
                    log_lifecycle=log_lifecycle,
                    effective_log_level=setup.effective_log_level,
                    app_logger=setup.app_logger,
                    function_name=setup.function_name,
                    function_id=setup.function_id,
                    app_id=setup.app_id,
                    runtime_id=setup.runtime_id,
                    caller_module_name=setup.caller_module_name,
                    filepath=setup.filepath,
                    func_lineno=setup.func_lineno,
                    current_call_chain=setup.current_call_chain,
                    prev_function_id=setup.prev_function_id,
                    prev_call_chain=setup.prev_call_chain,
                    prev_thread_id=setup.prev_thread_id,
                    context_token=setup.context_token,
                )

            return result

        @functools.wraps(decorated_func)
        async def async_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            setup = _setup_fun_watch_invocation(
                decorated_func, self, args,
                task_size_detect=task_size, log_lifecycle=log_lifecycle, log_level=log_level,
            )

            exc_occurred = False
            caught_error_type: str | None = None
            caught_error_message: str | None = None
            try:
                result = await decorated_func(self, *args, **kwargs)
            except Exception as exc:
                exc_occurred = True
                caught_error_type = type(exc).__name__
                caught_error_message = str(exc)
                _solved, _failed = setup.invocation_context.snapshot()
                setup.app_logger.exception(
                    "Unhandled exception in @fun_watch decorated function",
                    function_name=setup.function_name,
                    function_id=setup.function_id,
                    app_id=setup.app_id,
                    runtime=setup.runtime_id,
                    error_type=caught_error_type,
                    error_message=caught_error_message,
                    solved=_solved,
                    failed=_failed,
                    is_success=False,
                    log_id=setup.invocation_context.log_id,
                    module_name=setup.caller_module_name,
                    module_path=setup.filepath,
                    lineno=setup.func_lineno,
                    call_chain=setup.current_call_chain,
                )
                raise
            finally:
                _finalize_fun_watch(
                    invocation_context=setup.invocation_context,
                    registry=setup.registry,
                    start_time=setup.start_time,
                    exc_occurred=exc_occurred,
                    caught_error_type=caught_error_type,
                    caught_error_message=caught_error_message,
                    log_lifecycle=log_lifecycle,
                    effective_log_level=setup.effective_log_level,
                    app_logger=setup.app_logger,
                    function_name=setup.function_name,
                    function_id=setup.function_id,
                    app_id=setup.app_id,
                    runtime_id=setup.runtime_id,
                    caller_module_name=setup.caller_module_name,
                    filepath=setup.filepath,
                    func_lineno=setup.func_lineno,
                    current_call_chain=setup.current_call_chain,
                    prev_function_id=setup.prev_function_id,
                    prev_call_chain=setup.prev_call_chain,
                    prev_thread_id=setup.prev_thread_id,
                    context_token=setup.context_token,
                )

            return result

        if inspect.iscoroutinefunction(decorated_func):
            return async_wrapper
        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
