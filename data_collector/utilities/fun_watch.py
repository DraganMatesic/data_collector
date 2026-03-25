"""@fun_watch decorator for function-level performance monitoring.

Provides automatic function registration (AppFunctions) and per-function-per-runtime
aggregate metric recording (FunctionLog) with thread-safe context-local counters.
Binds ``function_id`` into structlog context for log correlation.
"""

from __future__ import annotations

import functools
import inspect
import logging
import statistics
import sys
import threading
from collections.abc import Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import structlog  # type: ignore[import-untyped]

from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.apps import AppFunctions
from data_collector.tables.log import FunctionLog
from data_collector.utilities.database.main import Database
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
    """Aggregate counters for a single function within a single runtime.

    One context exists per (function_hash, runtime) pair. Application code
    updates the active context through ``self._fun_watch.mark_solved()`` /
    ``self._fun_watch.mark_failed()``. The context accumulates metrics
    across all invocations of that function within the runtime.
    """

    __slots__ = (
        "solved", "failed", "task_size", "log_id", "call_count",
        "first_start_time", "_invocation_durations", "_counter_lock",
    )

    def __init__(self, task_size: int | None = None) -> None:
        self.solved: int = 0
        self.failed: int = 0
        self.task_size: int | None = task_size
        self.log_id: int | None = None
        self.call_count: int = 0
        self.first_start_time: datetime | None = None
        self._invocation_durations: list[float] = []
        self._counter_lock = threading.Lock()

    def increment_call_count(self) -> None:
        """Atomically increment call_count."""
        with self._counter_lock:
            self.call_count += 1

    def record_invocation_duration(self, duration_ms: float) -> None:
        """Record the duration of a single invocation in milliseconds."""
        with self._counter_lock:
            self._invocation_durations.append(duration_ms)

    def timing_snapshot(self) -> tuple[int, int, int, int, int]:
        """Return an atomic snapshot of timing statistics.

        Returns:
            Tuple of (total_elapsed_ms, average_elapsed_ms, median_elapsed_ms,
            min_elapsed_ms, max_elapsed_ms). All zeros when no durations recorded.
        """
        with self._counter_lock:
            if not self._invocation_durations:
                return 0, 0, 0, 0, 0
            durations = list(self._invocation_durations)
        total_elapsed_ms = int(sum(durations))
        average_elapsed_ms = int(total_elapsed_ms / len(durations))
        median_elapsed_ms = int(statistics.median(durations))
        min_elapsed_ms = int(min(durations))
        max_elapsed_ms = int(max(durations))
        return total_elapsed_ms, average_elapsed_ms, median_elapsed_ms, min_elapsed_ms, max_elapsed_ms

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
    """Proxy exposed as ``self._fun_watch`` that resolves active context-local state."""

    __slots__ = ("_registry",)

    def __init__(self, registry: FunWatchRegistry) -> None:
        self._registry = registry

    def mark_solved(self, count: int = 1) -> None:
        """Forward solved counter updates to the active invocation context."""
        self._registry.get_active_context().mark_solved(count)

    def mark_failed(self, count: int = 1) -> None:
        """Forward failed counter updates to the active invocation context."""
        self._registry.get_active_context().mark_failed(count)

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
    """Thread-safe singleton that manages function registration, aggregate contexts, and DB access.

    All mutable state lives here instead of in module-level globals.
    Access the singleton via ``FunWatchRegistry.instance()``.
    """

    _instance: FunWatchRegistry | None = None
    _instance_lock: threading.Lock = threading.Lock()

    def __init__(self) -> None:
        self._registered_functions: dict[str, bool] = {}
        self._registration_lock = threading.Lock()

        self._aggregate_contexts: dict[tuple[str, str], FunWatchContext] = {}
        self._aggregate_lock = threading.Lock()

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

    def set_system_db(self, database: Database) -> None:
        """Override the lazily-created system database with an externally managed instance.

        Used by example apps to redirect system table writes (AppFunctions, FunctionLog)
        to an isolated schema via ``schema_translate_map``.

        Args:
            database: A Database instance (typically with schema_translate_map applied).
        """
        with self._db_lock:
            self._system_db = database

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
            parent_chain = parent_structlog_context.get("call_chain", "")
            worker_name = getattr(func, "__qualname__", None) or getattr(func, "__name__", "worker")
            extended_chain = f"{parent_chain} -> {worker_name}" if parent_chain else worker_name
            structlog.contextvars.bind_contextvars(thread_id=threading.get_ident(), call_chain=extended_chain)
            try:
                return func(*args, **kwargs)
            finally:
                structlog.contextvars.unbind_contextvars(*parent_structlog_context.keys())
                self.unbind_context(token)

        return wrapped

    def wrap_with_thread_context(
        self,
        func: Callable[..., T],
        thread_context: FunWatchContext,
        thread_function_hash: str,
        application_logger: Any = None,
    ) -> Callable[..., T]:
        """Wrap callable so worker threads use a dedicated thread aggregate context.

        Unlike ``wrap_with_active_context`` which propagates the parent context,
        this binds the ``thread_context`` so that ``mark_solved()`` /
        ``mark_failed()`` update the thread callback's aggregate FunctionLog row.
        Per-invocation duration is measured and recorded on the thread context.
        Structlog contextvars are propagated from the parent for log correlation,
        with ``function_id`` overridden to the thread's function hash so that
        error logs point to the correct FunctionLog row.

        Args:
            func: The worker callable to wrap.
            thread_context: Aggregate context for the thread callback.
            thread_function_hash: Function hash for structlog function_id binding.
            application_logger: App-level logger with DatabaseHandler attached.
                When provided, unhandled exceptions are persisted to the Logs table.
                Falls back to the module-level logger (stderr only) when None.
        """
        parent_structlog_context = structlog.contextvars.get_contextvars()
        exception_logger = application_logger if application_logger is not None else logger

        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> T:
            token = self.bind_context(thread_context)
            structlog.contextvars.bind_contextvars(**parent_structlog_context)
            parent_chain = parent_structlog_context.get("call_chain", "")
            worker_name = getattr(func, "__qualname__", None) or getattr(func, "__name__", "worker")
            extended_chain = f"{parent_chain} -> {worker_name}" if parent_chain else worker_name
            structlog.contextvars.bind_contextvars(
                thread_id=threading.get_ident(),
                call_chain=extended_chain,
                function_id=thread_function_hash,
                function_name=worker_name,
            )
            invocation_start = datetime.now(UTC)
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                thread_context.mark_failed(1)
                traceback_frame = exc.__traceback__
                exception_module_path = ""
                exception_lineno = 0
                if traceback_frame is not None:
                    while traceback_frame.tb_next is not None:
                        traceback_frame = traceback_frame.tb_next
                    exception_module_path = traceback_frame.tb_frame.f_code.co_filename
                    exception_lineno = traceback_frame.tb_lineno
                exception_logger.exception(
                    f"Unhandled exception in thread callback {worker_name}",
                    module_name=Path(exception_module_path).name if exception_module_path else None,
                    module_path=exception_module_path or None,
                    lineno=exception_lineno or None,
                )
                raise
            finally:
                duration_ms = (datetime.now(UTC) - invocation_start).total_seconds() * 1000.0
                thread_context.record_invocation_duration(duration_ms)
                structlog.contextvars.unbind_contextvars(*parent_structlog_context.keys(), "function_name")
                self.unbind_context(token)

        return wrapped

    def try_get_active_context(self) -> FunWatchContext | None:
        """Return the currently active context, or None if none is bound."""
        return self._active_context.get()

    def ensure_context_proxy(self, instance: Any) -> None:
        """Attach the stable context proxy to instance as ``_fun_watch``."""
        if getattr(instance, "_fun_watch", None) is not self._context_proxy:
            instance._fun_watch = self._context_proxy

    def get_or_create_aggregate_context(
        self,
        function_hash: str,
        runtime_id: str,
        *,
        main_app: str,
        app_id: str,
        start_time: datetime,
        log_role: str = "function",
        caller_log_id: int | None = None,
    ) -> FunWatchContext:
        """Return the aggregate context for (function_hash, runtime_id), creating one on first call.

        Uses double-checked locking: fast-path read without lock, then atomic
        creation under ``_aggregate_lock`` on first access.  The FunctionLog
        INSERT happens inside the lock to guarantee exactly one row per function
        per runtime.
        """
        key = (function_hash, runtime_id)
        context = self._aggregate_contexts.get(key)
        if context is not None:
            return context

        with self._aggregate_lock:
            context = self._aggregate_contexts.get(key)
            if context is not None:
                return context

            context = FunWatchContext()
            context.first_start_time = start_time
            context.log_id = self.start_function_log(
                function_hash=function_hash,
                main_app=main_app,
                app_id=app_id,
                start_time=start_time,
                runtime_id=runtime_id,
                log_role=log_role,
                caller_log_id=caller_log_id,
            )
            self._aggregate_contexts[key] = context
            return context

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
                    sha = str(make_hash({
                        "function_hash": function_hash,
                        "function_name": function_name,
                        "filepath": filepath,
                        "app_id": app_id,
                    }))
                    record = AppFunctions(
                        function_hash=function_hash,
                        function_name=function_name,
                        filepath=filepath,
                        app_id=app_id,
                        first_seen=now,
                        last_seen=now,
                        sha=sha,
                    )
                    db.update_insert(
                        record,
                        session,
                        filter_cols=["function_hash"],
                    )
                self._registered_functions[function_hash] = True
            except Exception:
                logger.exception("Failed to register function", function_name=function_name)

    def register_thread(
        self,
        callback: Callable[..., Any],
        app_id: str,
        runtime_id: str,
        *,
        main_app: str | None = None,
        caller_log_id: int | None = None,
    ) -> tuple[FunWatchContext, str]:
        """Register a thread pool callback in AppFunctions and create its aggregate FunctionLog.

        Used by ``process_batch()`` to make thread callbacks visible in
        AppFunctions and FunctionLog with ``log_role='thread'``.

        Args:
            callback: The worker callable (e.g. ``self._scrape_page``).
            app_id: Application hash identifier.
            runtime_id: Current runtime hash.
            main_app: Root app hash (defaults to app_id).
            caller_log_id: FunctionLog id of the caller (for thread-to-caller navigability).

        Returns:
            Tuple of (aggregate FunWatchContext, function_hash for structlog binding).
        """
        function_name = getattr(callback, "__qualname__", None) or getattr(callback, "__name__", "thread_callback")
        function_hash = str(make_hash(app_id + function_name))
        filepath = inspect.getfile(callback) if hasattr(callback, "__code__") else ""

        self.register_function(function_hash, function_name, filepath, app_id)

        context = self.get_or_create_aggregate_context(
            function_hash,
            runtime_id,
            main_app=main_app or app_id,
            app_id=app_id,
            start_time=datetime.now(UTC),
            log_role="thread",
            caller_log_id=caller_log_id,
        )
        return context, function_hash

    def update_last_seen(self, function_hash: str) -> None:
        """Touch AppFunctions.last_seen for the given hash."""
        db = self._get_system_db()
        try:
            with db.create_session() as session:
                from sqlalchemy import select as sa_select
                stmt = sa_select(AppFunctions).where(AppFunctions.function_hash == function_hash)
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
        main_app: str,
        app_id: str,
        start_time: datetime,
        runtime_id: str,
        log_role: str,
        caller_log_id: int | None = None,
    ) -> int | None:
        """INSERT a FunctionLog row for a new (function_hash, runtime) aggregate."""
        db = self._get_system_db()
        try:
            record = FunctionLog(
                function_hash=function_hash,
                main_app=main_app,
                app_id=app_id,
                start_time=start_time,
                runtime=runtime_id,
                log_role=log_role,
                caller_log_id=caller_log_id,
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
        call_count: int,
        end_time: datetime,
        total_elapsed_ms: int,
        average_elapsed_ms: int,
        median_elapsed_ms: int,
        min_elapsed_ms: int,
        max_elapsed_ms: int,
        exc_occurred: bool = False,
        task_size: int | None = None,
    ) -> None:
        """UPDATE an existing FunctionLog row with aggregate metrics."""
        if log_id is None:
            return
        db = self._get_system_db()
        try:
            with db.create_session() as session:
                record = session.get(FunctionLog, log_id)
                if record is not None:
                    record.solved = solved  # type: ignore[assignment]
                    record.failed = failed  # type: ignore[assignment]
                    record.call_count = call_count  # type: ignore[assignment]
                    record.processed_count = solved + failed  # type: ignore[assignment]
                    record.is_success = not exc_occurred and failed == 0  # type: ignore[assignment]
                    record.end_time = end_time  # type: ignore[assignment]
                    record.total_elapsed_ms = total_elapsed_ms  # type: ignore[assignment]
                    record.average_elapsed_ms = average_elapsed_ms  # type: ignore[assignment]
                    record.median_elapsed_ms = median_elapsed_ms  # type: ignore[assignment]
                    record.min_elapsed_ms = min_elapsed_ms  # type: ignore[assignment]
                    record.max_elapsed_ms = max_elapsed_ms  # type: ignore[assignment]
                    if task_size is not None:
                        record.task_size = task_size  # type: ignore[assignment]
                    session.commit()
        except Exception:
            logger.exception("Failed to complete FunctionLog", log_id=log_id)


def _finalize_fun_watch(
    *,
    aggregate_context: FunWatchContext,
    registry: FunWatchRegistry,
    invocation_start_time: datetime,
    exc_occurred: bool,
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
    prev_function_name: Any,
    context_token: Token[FunWatchContext | None],
) -> None:
    """Shared finalization logic for sync and async @fun_watch wrappers."""
    try:
        end_time = datetime.now(UTC)
        duration_ms = (end_time - invocation_start_time).total_seconds() * 1000.0
        aggregate_context.record_invocation_duration(duration_ms)

        solved, failed = aggregate_context.snapshot()
        total_elapsed_ms, average_elapsed_ms, median_elapsed_ms, min_elapsed_ms, max_elapsed_ms = (
            aggregate_context.timing_snapshot()
        )
        registry.complete_function_log(
            log_id=aggregate_context.log_id,
            solved=solved,
            failed=failed,
            call_count=aggregate_context.call_count,
            end_time=end_time,
            total_elapsed_ms=total_elapsed_ms,
            average_elapsed_ms=average_elapsed_ms,
            median_elapsed_ms=median_elapsed_ms,
            min_elapsed_ms=min_elapsed_ms,
            max_elapsed_ms=max_elapsed_ms,
            exc_occurred=exc_occurred,
            task_size=aggregate_context.task_size,
        )
        registry.update_last_seen(function_id)
        if not exc_occurred and log_lifecycle:
            duration_s = duration_ms / 1000.0
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
                task_size=aggregate_context.task_size,
                call_count=aggregate_context.call_count,
                duration_s=duration_s,
                log_id=aggregate_context.log_id,
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
        if prev_function_name is not None:
            structlog.contextvars.bind_contextvars(function_name=prev_function_name)
        else:
            structlog.contextvars.unbind_contextvars("function_name")
        registry.unbind_context(context_token)


@dataclass
class _FunWatchSetupResult:
    """Holds all computed state from the shared setup phase of @fun_watch."""

    registry: FunWatchRegistry
    aggregate_context: FunWatchContext
    context_token: Token[FunWatchContext | None]
    invocation_start_time: datetime
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
    prev_function_name: Any = None


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

    Performs function registration, aggregate context retrieval/creation,
    structlog context management, and lifecycle logging.  Returns all state
    needed by the caller to invoke the decorated function and finalize.
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

    registry.ensure_context_proxy(instance)

    thread_id = threading.get_ident()
    invocation_start_time = datetime.now(UTC)

    aggregate_context = registry.get_or_create_aggregate_context(
        function_id,
        runtime_id,
        main_app=main_app,
        app_id=app_id,
        start_time=invocation_start_time,
    )
    aggregate_context.increment_call_count()

    if detected_task_size is not None:
        aggregate_context.task_size = detected_task_size

    context_token = registry.bind_context(aggregate_context)

    previous_structlog_context = structlog.contextvars.get_contextvars()
    prev_function_id = previous_structlog_context.get("function_id")
    prev_call_chain = previous_structlog_context.get("call_chain")
    prev_thread_id = previous_structlog_context.get("thread_id")
    prev_function_name = previous_structlog_context.get("function_name")
    if prev_call_chain:
        current_call_chain = f"{prev_call_chain} -> {chain_label}"
    else:
        root_caller = _find_root_caller()
        if root_caller and root_caller != function_name:
            current_call_chain = f"{root_caller} -> {chain_label}"
        else:
            current_call_chain = chain_label
    structlog.contextvars.bind_contextvars(
        function_id=function_id, call_chain=current_call_chain, thread_id=thread_id,
        function_name=chain_label,
    )

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
            log_id=aggregate_context.log_id,
            call_count=aggregate_context.call_count,
            log_role="function",
            module_name=caller_module_name,
            module_path=filepath,
            lineno=func_lineno,
            call_chain=current_call_chain,
            **lifecycle_started_extras,
        )

    return _FunWatchSetupResult(
        registry=registry,
        aggregate_context=aggregate_context,
        context_token=context_token,
        invocation_start_time=invocation_start_time,
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
        prev_function_name=prev_function_name,
    )


def fun_watch(
    func: Any = None,
    *,
    task_size: bool = True,
    log_lifecycle: bool = True,
    log_level: int | None = None,
) -> Any:
    """Decorator that monitors function execution and records aggregate metrics to DB.

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
            try:
                result = decorated_func(self, *args, **kwargs)
            except Exception as exc:
                exc_occurred = True
                _solved, _failed = setup.aggregate_context.snapshot()
                setup.app_logger.exception(
                    "Unhandled exception in @fun_watch decorated function",
                    function_name=setup.function_name,
                    function_id=setup.function_id,
                    app_id=setup.app_id,
                    runtime=setup.runtime_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    solved=_solved,
                    failed=_failed,
                    is_success=False,
                    log_id=setup.aggregate_context.log_id,
                    module_name=setup.caller_module_name,
                    module_path=setup.filepath,
                    lineno=setup.func_lineno,
                    call_chain=setup.current_call_chain,
                )
                raise
            finally:
                _finalize_fun_watch(
                    aggregate_context=setup.aggregate_context,
                    registry=setup.registry,
                    invocation_start_time=setup.invocation_start_time,
                    exc_occurred=exc_occurred,
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
                    prev_function_name=setup.prev_function_name,
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
            try:
                result = await decorated_func(self, *args, **kwargs)
            except Exception as exc:
                exc_occurred = True
                _solved, _failed = setup.aggregate_context.snapshot()
                setup.app_logger.exception(
                    "Unhandled exception in @fun_watch decorated function",
                    function_name=setup.function_name,
                    function_id=setup.function_id,
                    app_id=setup.app_id,
                    runtime=setup.runtime_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    solved=_solved,
                    failed=_failed,
                    is_success=False,
                    log_id=setup.aggregate_context.log_id,
                    module_name=setup.caller_module_name,
                    module_path=setup.filepath,
                    lineno=setup.func_lineno,
                    call_chain=setup.current_call_chain,
                )
                raise
            finally:
                _finalize_fun_watch(
                    aggregate_context=setup.aggregate_context,
                    registry=setup.registry,
                    invocation_start_time=setup.invocation_start_time,
                    exc_occurred=exc_occurred,
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
                    prev_function_name=setup.prev_function_name,
                    context_token=setup.context_token,
                )

            return result

        if inspect.iscoroutinefunction(decorated_func):
            return async_wrapper
        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
