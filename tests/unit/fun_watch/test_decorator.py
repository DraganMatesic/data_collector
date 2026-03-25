"""Unit tests for @fun_watch decorator behavior."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest
import structlog  # type: ignore[import-untyped]

from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry, fun_watch

_REGISTRY = "data_collector.utilities.fun_watch.FunWatchRegistry"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeApp(FunWatchMixin):
    """Minimal app instance for decorator testing."""

    def __init__(
        self,
        app_id: str = "test_app_hash",
        runtime: str = "test_runtime_hash",
        main_app: str = "",
    ) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.main_app = main_app

    @fun_watch
    def process_items(self, items: list[str]) -> int:
        for _item in items:
            self._fun_watch.mark_solved()
        return len(items)

    @fun_watch
    def failing_method(self, items: list[str]) -> None:
        self._fun_watch.mark_solved(1)
        raise ValueError("test error")

    @fun_watch
    def no_args_method(self) -> str:
        return "done"

    @fun_watch
    def non_sized_arg(self, value: int) -> int:
        return value


class NestedApp(FunWatchMixin):
    """App with nested decorated methods to validate context restoration."""

    def __init__(self) -> None:
        self.app_id = "nested_app_hash"
        self.runtime = "nested_runtime_hash"

    @fun_watch
    def inner(self, items: list[str]) -> int:
        for _item in items:
            self._fun_watch.mark_solved()
        return len(items)

    @fun_watch
    def outer(self, outer_items: list[str], inner_items: list[str]) -> int:
        for _item in outer_items:
            self._fun_watch.mark_solved()
        _ = self.inner(inner_items)
        for _item in outer_items:
            self._fun_watch.mark_failed()
        return len(outer_items) + len(inner_items)


class NestedExceptionApp(FunWatchMixin):
    """App with nested decorated methods where inner call raises an exception."""

    def __init__(self) -> None:
        self.app_id = "nested_exception_app_hash"
        self.runtime = "nested_exception_runtime_hash"

    @fun_watch
    def inner_fail(self, items: list[str]) -> None:
        self._fun_watch.mark_failed()
        raise ValueError("nested failure")

    @fun_watch
    def outer_handles_inner_failure(self, outer_items: list[str], inner_items: list[str]) -> int:
        for _item in outer_items:
            self._fun_watch.mark_solved()
        try:
            self.inner_fail(inner_items)
        except ValueError:
            self._fun_watch.mark_failed(len(inner_items))
        return len(outer_items)


class NestedFanOutApp(FunWatchMixin):
    """App combining nested @fun_watch calls with child-thread fan-out."""

    def __init__(self) -> None:
        self.app_id = "nested_fan_out_app_hash"
        self.runtime = "nested_fan_out_runtime_hash"

    @fun_watch
    def inner(self, items: list[str]) -> int:
        for _item in items:
            self._fun_watch.mark_solved()
        return len(items)

    @fun_watch
    def outer(self, worker_count: int, inner_items: list[str], fail_count: int) -> int:
        registry = FunWatchRegistry.instance()

        def worker() -> None:
            self._fun_watch.mark_solved()

        wrapped_worker = registry.wrap_with_active_context(worker)
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(wrapped_worker) for _ in range(worker_count)]
            for future in futures:
                future.result()

        _ = self.inner(inner_items)

        for _ in range(fail_count):
            self._fun_watch.mark_failed()

        return worker_count + len(inner_items)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRegistration:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_calls_register_on_first_invocation(
        self,
        mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        mock_register.assert_called_once()
        call_args = mock_register.call_args
        assert call_args[0][1] == "process_items"

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_calls_register_on_each_invocation(
        self,
        mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        app.process_items(["b"])
        assert mock_register.call_count == 2


class TestFunctionLog:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_inserts_log_on_success(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b"])
        mock_start_log.assert_called_once()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_solved_count(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["solved"] == 3

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_task_size_in_complete(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["task_size"] == 3

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_none_for_non_sized_arg(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.non_sized_arg(42)
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["task_size"] is None

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_none_when_no_args(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.no_args_method()
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["task_size"] is None

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_app_id_and_runtime(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp(app_id="my_app", runtime="my_runtime")
        app.process_items(["x"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["app_id"] == "my_app"
        assert call_kwargs["runtime_id"] == "my_runtime"

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_main_app_defaults_to_app_id(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp(app_id="my_app", runtime="rt")
        app.process_items(["x"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["main_app"] == "my_app"

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_main_app_from_self(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp(app_id="child", runtime="rt", main_app="root_app")
        app.process_items(["x"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["main_app"] == "root_app"

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_complete_includes_call_count(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["call_count"] == 1

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_complete_includes_timing_metrics(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        call_kwargs = mock_complete_log.call_args[1]
        assert "total_elapsed_ms" in call_kwargs
        assert "average_elapsed_ms" in call_kwargs
        assert "median_elapsed_ms" in call_kwargs
        assert "min_elapsed_ms" in call_kwargs
        assert "max_elapsed_ms" in call_kwargs

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_start_function_log_has_log_role_function(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["log_role"] == "function"

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_start_function_log_has_no_parent_fields(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        call_kwargs = mock_start_log.call_args[1]
        assert "parent_log_id" not in call_kwargs
        assert "execution_order" not in call_kwargs
        assert "thread_execution_order" not in call_kwargs
        assert "thread_id" not in call_kwargs

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_aggregate_context_reused_across_invocations(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        """Second invocation of same function reuses aggregate context -- one INSERT, two UPDATEs."""
        app = FakeApp()
        app.process_items(["a"])
        app.process_items(["b", "c"])
        assert mock_start_log.call_count == 1
        assert mock_complete_log.call_count == 2
        last_kwargs = mock_complete_log.call_args[1]
        assert last_kwargs["call_count"] == 2
        assert last_kwargs["solved"] == 3


class TestExceptionHandling:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_still_records_log_on_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError, match="test error"):
            app.failing_method(["x"])
        mock_complete_log.assert_called_once()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_partial_solved_on_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["solved"] == 1

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_re_raises_original_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError, match="test error"):
            app.failing_method(["x"])


class TestReturnValue:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_returns_function_result(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        result = app.process_items(["a", "b"])
        assert result == 2

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_returns_string_result(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        result = app.no_args_method()
        assert result == "done"


class TestNestedInvocations:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_success_each_function_gets_own_aggregate(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = NestedApp()
        result = app.outer(["a", "b"], ["x", "y", "z"])

        assert result == 5
        assert mock_start_log.call_count == 2
        assert mock_complete_log.call_count == 2

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_exception_restores_outer_context(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = NestedExceptionApp()
        result = app.outer_handles_inner_failure(["a", "b"], ["x", "y", "z"])

        assert result == 2
        assert mock_start_log.call_count == 2
        assert mock_complete_log.call_count == 2

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_fan_out_keeps_contexts_isolated(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
    ) -> None:
        app = NestedFanOutApp()
        result = app.outer(6, ["a", "b", "c"], 2)

        assert result == 9
        assert mock_start_log.call_count == 2
        assert mock_complete_log.call_count == 2


class TestValidation:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    def test_raises_without_app_id(self) -> None:
        class BadApp:
            runtime = "some_runtime"

            @fun_watch
            def do_thing(self) -> None:
                pass

        app = BadApp()
        with pytest.raises(TypeError, match="app_id"):
            app.do_thing()

    def test_raises_without_runtime(self) -> None:
        class BadApp:
            app_id = "some_app"

            @fun_watch
            def do_thing(self) -> None:
                pass

        app = BadApp()
        with pytest.raises(TypeError, match="runtime"):
            app.do_thing()


class TestLastSeen:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_updates_last_seen_on_success(
        self,
        _mock_register: MagicMock,
        mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        mock_last_seen.assert_called_once()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_updates_last_seen_on_exception(
        self,
        _mock_register: MagicMock,
        mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        mock_last_seen.assert_called_once()


class TestStructlogContextBinding:
    """Verify that @fun_watch binds/unbinds function_id in structlog contextvars."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()
        structlog.contextvars.clear_contextvars()

    def teardown_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_binds_function_id_in_structlog_context(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        captured: dict[str, str] = {}

        class CapturingApp(FunWatchMixin):
            app_id = "ctx_test_app"
            runtime = "ctx_test_runtime"

            @fun_watch
            def do_work(self, items: list[str]) -> None:
                structlog_context = structlog.contextvars.get_contextvars()
                captured["function_id"] = structlog_context.get("function_id", "")

        app = CapturingApp()
        app.do_work(["a"])
        assert captured["function_id"] != ""

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_unbinds_function_id_after_return(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        class SimpleApp(FunWatchMixin):
            app_id = "unbind_test_app"
            runtime = "unbind_test_runtime"

            @fun_watch
            def do_work(self) -> None:
                pass

        app = SimpleApp()
        app.do_work()
        structlog_context = structlog.contextvars.get_contextvars()
        assert "function_id" not in structlog_context

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_binds_thread_id_in_structlog_context(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        captured: dict[str, int] = {}

        class CapturingApp(FunWatchMixin):
            app_id = "thread_ctx_app"
            runtime = "thread_ctx_runtime"

            @fun_watch
            def do_work(self, items: list[str]) -> None:
                structlog_context = structlog.contextvars.get_contextvars()
                captured["thread_id"] = structlog_context.get("thread_id", 0)

        app = CapturingApp()
        app.do_work(["a"])
        assert captured["thread_id"] == threading.get_ident()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_unbinds_thread_id_after_return(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        class SimpleApp(FunWatchMixin):
            app_id = "unbind_thread_app"
            runtime = "unbind_thread_runtime"

            @fun_watch
            def do_work(self) -> None:
                pass

        app = SimpleApp()
        app.do_work()
        structlog_context = structlog.contextvars.get_contextvars()
        assert "thread_id" not in structlog_context

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_calls_restore_outer_function_id(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        captured_outer_before: dict[str, str] = {}
        captured_inner: dict[str, str] = {}
        captured_outer_after: dict[str, str] = {}

        class NestingApp(FunWatchMixin):
            app_id = "nesting_test_app"
            runtime = "nesting_test_runtime"

            @fun_watch
            def inner(self) -> None:
                structlog_context = structlog.contextvars.get_contextvars()
                captured_inner["function_id"] = structlog_context.get("function_id", "")

            @fun_watch
            def outer(self) -> None:
                structlog_context = structlog.contextvars.get_contextvars()
                captured_outer_before["function_id"] = structlog_context.get("function_id", "")
                self.inner()
                structlog_context = structlog.contextvars.get_contextvars()
                captured_outer_after["function_id"] = structlog_context.get("function_id", "")

        app = NestingApp()
        app.outer()

        assert captured_outer_before["function_id"] != ""
        assert captured_inner["function_id"] != ""
        assert captured_inner["function_id"] != captured_outer_before["function_id"]
        assert captured_outer_after["function_id"] == captured_outer_before["function_id"]

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_call_chain_bound_during_execution(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        captured: dict[str, str] = {}

        class ChainApp(FunWatchMixin):
            app_id = "chain_test_app"
            runtime = "chain_test_runtime"

            @fun_watch
            def do_work(self, items: list[str]) -> None:
                structlog_context = structlog.contextvars.get_contextvars()
                captured["call_chain"] = structlog_context.get("call_chain", "")

        app = ChainApp()
        app.do_work(["a"])
        assert captured["call_chain"].endswith("-> ChainApp.do_work")
        assert "ChainApp.do_work" in captured["call_chain"]

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_call_chain_shows_parent_and_child(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        captured_inner: dict[str, str] = {}

        class NestingChainApp(FunWatchMixin):
            app_id = "nesting_chain_app"
            runtime = "nesting_chain_runtime"

            @fun_watch
            def inner(self) -> None:
                structlog_context = structlog.contextvars.get_contextvars()
                captured_inner["call_chain"] = structlog_context.get("call_chain", "")

            @fun_watch
            def outer(self) -> None:
                self.inner()

        app = NestingChainApp()
        app.outer()
        assert captured_inner["call_chain"].endswith("NestingChainApp.outer -> NestingChainApp.inner")

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_call_chain_restored_after_nested_call(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        captured_before: dict[str, str] = {}
        captured_after: dict[str, str] = {}

        class RestoreChainApp(FunWatchMixin):
            app_id = "restore_chain_app"
            runtime = "restore_chain_runtime"

            @fun_watch
            def inner(self) -> None:
                pass

            @fun_watch
            def outer(self) -> None:
                structlog_context = structlog.contextvars.get_contextvars()
                captured_before["call_chain"] = structlog_context.get("call_chain", "")
                self.inner()
                structlog_context = structlog.contextvars.get_contextvars()
                captured_after["call_chain"] = structlog_context.get("call_chain", "")

        app = RestoreChainApp()
        app.outer()
        assert captured_before["call_chain"].endswith("RestoreChainApp.outer")
        assert captured_after["call_chain"] == captured_before["call_chain"]

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_call_chain_unbound_after_return(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        class UnbindChainApp(FunWatchMixin):
            app_id = "unbind_chain_app"
            runtime = "unbind_chain_runtime"

            @fun_watch
            def do_work(self) -> None:
                pass

        app = UnbindChainApp()
        app.do_work()
        structlog_context = structlog.contextvars.get_contextvars()
        assert "call_chain" not in structlog_context

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_unbinds_function_id_on_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        class FailingApp(FunWatchMixin):
            app_id = "fail_ctx_app"
            runtime = "fail_ctx_runtime"

            @fun_watch
            def do_work(self) -> None:
                raise ValueError("boom")

        app = FailingApp()
        with pytest.raises(ValueError):
            app.do_work()
        structlog_context = structlog.contextvars.get_contextvars()
        assert "function_id" not in structlog_context


class TestExceptionLogging:
    """Verify that @fun_watch auto-logs unhandled exceptions via structlog."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()
        structlog.contextvars.clear_contextvars()

    def teardown_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_unhandled_exception_is_logged_via_structlog(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        class FailApp(FunWatchMixin):
            app_id = "log_exc_app"
            runtime = "log_exc_runtime"

            @fun_watch
            def do_work(self) -> None:
                raise RuntimeError("something broke")

        app = FailApp()
        with pytest.raises(RuntimeError, match="something broke"):
            app.do_work()
        mock_logger.exception.assert_called_once()
        call_kwargs = mock_logger.exception.call_args[1]
        assert call_kwargs["function_name"] == "do_work"
        assert call_kwargs["app_id"] == "log_exc_app"
        assert call_kwargs["error_type"] == "RuntimeError"
        assert call_kwargs["error_message"] == "something broke"
        assert call_kwargs["is_success"] is False


class TestLifecycleLogging:
    """Verify @fun_watch lifecycle logging (started, completed, exception routing)."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()
        structlog.contextvars.clear_contextvars()

    def teardown_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_logs_function_started_on_entry(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        assert started_calls[0][1]["function_name"] == "process_items"

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_function_started_omits_task_size_when_none(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.no_args_method()
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        assert "task_size" not in started_calls[0][1]

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_function_started_includes_task_size_when_detected(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        assert started_calls[0][1]["task_size"] == 3

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_logs_function_completed_on_success(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        completed_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function completed"]
        assert len(completed_calls) == 1
        kwargs = completed_calls[0][1]
        assert kwargs["function_name"] == "process_items"
        assert kwargs["solved"] == 3
        assert kwargs["failed"] == 0
        assert kwargs["processed_count"] == 3
        assert kwargs["is_success"] is True
        assert kwargs["task_size"] == 3
        assert kwargs["call_count"] == 1
        assert "duration_s" in kwargs

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_no_completed_log_on_exception(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        completed_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function completed"]
        assert len(completed_calls) == 0
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_lifecycle_logs_include_call_chain(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert started_calls[0][1]["call_chain"].endswith("FakeApp.process_items")
        completed_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function completed"]
        assert completed_calls[0][1]["call_chain"].endswith("FakeApp.process_items")

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_lifecycle_logs_include_module_name_and_path(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        kwargs = started_calls[0][1]
        assert kwargs["module_name"] == "test_decorator.py"
        assert kwargs["module_path"].endswith("test_decorator.py")
        completed_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function completed"]
        assert len(completed_calls) == 1
        kwargs = completed_calls[0][1]
        assert kwargs["module_name"] == "test_decorator.py"
        assert kwargs["module_path"].endswith("test_decorator.py")

    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    @patch("data_collector.utilities.fun_watch.logger")
    def test_exception_log_includes_module_name_and_path(
        self,
        mock_logger: MagicMock,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        kwargs = mock_logger.exception.call_args[1]
        assert kwargs["module_name"] == "test_decorator.py"
        assert kwargs["module_path"].endswith("test_decorator.py")
