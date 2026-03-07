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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        mock_register.assert_called_once()
        call_args = mock_register.call_args
        assert call_args[0][1] == "process_items"

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        app.process_items(["b"])
        assert mock_register.call_count == 2


class TestFunctionLog:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b"])
        mock_start_log.assert_called_once()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_solved_count(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["solved"] == 3

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_task_size(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["task_size"] == 3

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_none_for_non_sized_arg(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.non_sized_arg(42)
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["task_size"] is None

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_none_when_no_args(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.no_args_method()
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["task_size"] is None

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_app_id_and_runtime(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp(app_id="my_app", runtime="my_runtime")
        app.process_items(["x"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["app_id"] == "my_app"
        assert call_kwargs["runtime_id"] == "my_runtime"

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_main_app_defaults_to_app_id(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp(app_id="my_app", runtime="rt")
        app.process_items(["x"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["main_app"] == "my_app"

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_main_app_from_self(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp(app_id="child", runtime="rt", main_app="root_app")
        app.process_items(["x"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["main_app"] == "root_app"


class TestExceptionHandling:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_still_records_log_on_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError, match="test error"):
            app.failing_method(["x"])
        mock_complete_log.assert_called_once()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_partial_solved_on_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        call_kwargs = mock_complete_log.call_args[1]
        assert call_kwargs["solved"] == 1

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError, match="test error"):
            app.failing_method(["x"])


class TestReturnValue:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        result = app.process_items(["a", "b"])
        assert result == 2

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        result = app.no_args_method()
        assert result == "done"


class TestNestedInvocations:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_success_restores_outer_context(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = NestedApp()
        result = app.outer(["a", "b"], ["x", "y", "z"])

        assert result == 5
        assert mock_start_log.call_count == 2
        assert mock_complete_log.call_count == 2

        start_rows = [call[1] for call in mock_start_log.call_args_list]
        complete_rows = [call[1] for call in mock_complete_log.call_args_list]
        assert any(row["task_size"] == 3 for row in start_rows)
        assert any(row["task_size"] == 2 for row in start_rows)
        assert any(row["solved"] == 3 and row["failed"] == 0 for row in complete_rows)
        assert any(row["solved"] == 2 and row["failed"] == 2 for row in complete_rows)

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = NestedExceptionApp()
        result = app.outer_handles_inner_failure(["a", "b"], ["x", "y", "z"])

        assert result == 2
        assert mock_start_log.call_count == 2
        assert mock_complete_log.call_count == 2

        start_rows = [call[1] for call in mock_start_log.call_args_list]
        complete_rows = [call[1] for call in mock_complete_log.call_args_list]
        assert any(row["task_size"] == 3 for row in start_rows)
        assert any(row["task_size"] == 2 for row in start_rows)
        assert any(row["solved"] == 0 and row["failed"] == 1 for row in complete_rows)
        assert any(row["solved"] == 2 and row["failed"] == 3 for row in complete_rows)

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_fan_out_keeps_outer_and_inner_counters_isolated(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = NestedFanOutApp()
        result = app.outer(6, ["a", "b", "c"], 2)

        assert result == 9
        assert mock_start_log.call_count == 2
        assert mock_complete_log.call_count == 2

        start_rows = [call[1] for call in mock_start_log.call_args_list]
        complete_rows = [call[1] for call in mock_complete_log.call_args_list]
        assert any(row["task_size"] == 3 for row in start_rows)
        assert any(row["task_size"] is None for row in start_rows)
        assert any(row["solved"] == 3 and row["failed"] == 0 for row in complete_rows)
        assert any(row["solved"] == 6 and row["failed"] == 2 for row in complete_rows)


class TestParentLogTracking:
    """Verify parent_log_id and log_role are set correctly for nested @fun_watch calls."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()
        structlog.contextvars.clear_contextvars()

    def teardown_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_top_level_has_no_parent(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        call_kwargs = mock_start_log.call_args[1]
        assert call_kwargs["parent_log_id"] is None
        assert call_kwargs["log_role"] == "single"

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_child_has_parent_log_id(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = NestedApp()
        app.outer(["a"], ["x"])
        start_rows = [call[1] for call in mock_start_log.call_args_list]
        outer_row = next(r for r in start_rows if r["log_role"] != "child")
        child_row = next(r for r in start_rows if r["log_role"] == "child")
        assert outer_row["parent_log_id"] is None
        assert child_row["parent_log_id"] == 1

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_parent_role_updated_when_child_detected(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        mock_parent_role: MagicMock,
    ) -> None:
        app = NestedApp()
        app.outer(["a"], ["x"])
        mock_parent_role.assert_called_once_with(1)


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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        mock_last_seen.assert_called_once()

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        assert started_calls[0][1]["function_name"] == "process_items"

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.no_args_method()
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        assert "task_size" not in started_calls[0][1]

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        assert started_calls[0][1]["task_size"] == 3

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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
        assert "duration_s" in kwargs

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        completed_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function completed"]
        assert len(completed_calls) == 0
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        started_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function started"]
        assert started_calls[0][1]["call_chain"].endswith("FakeApp.process_items")
        completed_calls = [c for c in mock_logger.log.call_args_list if c[0][1] == "Function completed"]
        assert completed_calls[0][1]["call_chain"].endswith("FakeApp.process_items")

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
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

    @patch(f"{_REGISTRY}.update_parent_log_role")
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
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        kwargs = mock_logger.exception.call_args[1]
        assert kwargs["module_name"] == "test_decorator.py"
        assert kwargs["module_path"].endswith("test_decorator.py")

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_uses_app_logger_when_available(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_app_logger = MagicMock()

        class AppWithLogger(FunWatchMixin):
            app_id = "logger_test_app"
            runtime = "logger_test_runtime"
            logger = mock_app_logger

            @fun_watch
            def do_work(self, items: list[str]) -> int:
                for _item in items:
                    self._fun_watch.mark_solved()
                return len(items)

        app = AppWithLogger()
        app.do_work(["a", "b"])
        started_calls = [c for c in mock_app_logger.log.call_args_list if c[0][1] == "Function started"]
        assert len(started_calls) == 1
        completed_calls = [c for c in mock_app_logger.log.call_args_list if c[0][1] == "Function completed"]
        assert len(completed_calls) == 1
        assert completed_calls[0][1]["solved"] == 2

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_exception_logged_via_app_logger(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_app_logger = MagicMock()

        class AppWithLogger(FunWatchMixin):
            app_id = "exc_logger_app"
            runtime = "exc_logger_runtime"
            logger = mock_app_logger

            @fun_watch
            def do_fail(self) -> None:
                raise RuntimeError("boom")

        app = AppWithLogger()
        with pytest.raises(RuntimeError):
            app.do_fail()
        mock_app_logger.exception.assert_called_once()
        assert mock_app_logger.exception.call_args[0][0] == "Unhandled exception in @fun_watch decorated function"


class TestErrorColumnPropagation:
    """Verify that error details are forwarded to complete_function_log."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_unhandled_exception_sets_error_type_and_message(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError, match="test error"):
            app.failing_method(["x"])
        kw = mock_complete_log.call_args[1]
        assert kw["exc_occurred"] is True
        assert kw["error_type"] == "ValueError"
        assert kw["error_message"] == "test error"

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_success_has_no_error_info(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b"])
        kw = mock_complete_log.call_args[1]
        assert kw["exc_occurred"] is False
        assert kw["error_type"] is None
        assert kw["error_message"] is None

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_mark_failed_with_error_type_propagates_item_errors(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        class ErrorTrackingApp(FunWatchMixin):
            app_id = "err_track_app"
            runtime = "err_track_runtime"

            @fun_watch
            def partial_fail(self, items: list[str]) -> None:
                for i, _item in enumerate(items):
                    if i >= 2:
                        self._fun_watch.mark_failed(
                            len(items) - i,
                            error_type="ProcessingError",
                            error_message="item failed",
                        )
                        raise RuntimeError("partial failure")
                    self._fun_watch.mark_solved()

        app = ErrorTrackingApp()
        with pytest.raises(RuntimeError):
            app.partial_fail(["a", "b", "c", "d"])
        kw = mock_complete_log.call_args[1]
        assert kw["exc_occurred"] is True
        assert kw["error_type"] == "RuntimeError"
        assert kw["error_message"] == "partial failure"
        assert kw["item_error_count"] == 2
        assert kw["item_error_types_json"] is not None
        assert "ProcessingError" in kw["item_error_types_json"]


class TestTaskSizeOptOut:
    """Verify task_size=False skips auto-detection from list args."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()
        structlog.contextvars.clear_contextvars()

    def teardown_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_false_skips_detection(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"

            @fun_watch(task_size=False)
            def store(self, records: list[str]) -> None:
                pass

        app = App()
        app.store(["a", "b", "c"])
        kw = mock_start_log.call_args[1]
        assert kw["task_size"] is None

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_bare_decorator_detects_task_size(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"

            @fun_watch
            def store(self, records: list[str]) -> None:
                pass

        app = App()
        app.store(["a", "b", "c"])
        kw = mock_start_log.call_args[1]
        assert kw["task_size"] == 3

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_true_explicit(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"

            @fun_watch(task_size=True)
            def store(self, records: list[str]) -> None:
                pass

        app = App()
        app.store(["a", "b", "c"])
        kw = mock_start_log.call_args[1]
        assert kw["task_size"] == 3


class TestLogLifecycleControl:
    """Verify log_lifecycle and log_level parameters."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()
        structlog.contextvars.clear_contextvars()

    def teardown_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_lifecycle_false_suppresses_lifecycle_logs(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch(log_lifecycle=False)
            def process(self) -> None:
                pass

        app = App()
        app.process()
        mock_logger.log.assert_not_called()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_lifecycle_false_still_writes_function_log(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = MagicMock()

            @fun_watch(log_lifecycle=False)
            def process(self) -> None:
                pass

        app = App()
        app.process()
        mock_start_log.assert_called_once()
        mock_complete_log.assert_called_once()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_lifecycle_false_still_logs_exceptions(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch(log_lifecycle=False)
            def process(self) -> None:
                raise ValueError("test")

        app = App()
        with pytest.raises(ValueError):
            app.process()
        mock_logger.exception.assert_called_once()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_lifecycle_true_default(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch
            def process(self) -> None:
                pass

        app = App()
        app.process()
        log_calls = mock_logger.log.call_args_list
        assert len(log_calls) == 2
        assert log_calls[0][0][1] == "Function started"
        assert log_calls[1][0][1] == "Function completed"

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_level_info_emits_at_info(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        import logging

        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch(log_level=logging.INFO)
            def process(self) -> None:
                pass

        app = App()
        app.process()
        log_calls = mock_logger.log.call_args_list
        assert log_calls[0][0][0] == logging.INFO
        assert log_calls[1][0][0] == logging.INFO

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_level_does_not_affect_exception_log(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        import logging

        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch(log_level=logging.WARNING)
            def process(self) -> None:
                raise RuntimeError("boom")

        app = App()
        with pytest.raises(RuntimeError):
            app.process()
        mock_logger.exception.assert_called_once()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_level_none_uses_registry_default(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        import logging

        FunWatchRegistry.instance().set_default_lifecycle_log_level(logging.INFO)
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch
            def process(self) -> None:
                pass

        app = App()
        app.process()
        log_calls = mock_logger.log.call_args_list
        assert log_calls[0][0][0] == logging.INFO

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_log_level_explicit_overrides_registry(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        import logging

        FunWatchRegistry.instance().set_default_lifecycle_log_level(logging.INFO)
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch(log_level=logging.WARNING)
            def process(self) -> None:
                pass

        app = App()
        app.process()
        log_calls = mock_logger.log.call_args_list
        assert log_calls[0][0][0] == logging.WARNING


class TestExecutionOrderCounters:
    """Verify global vs per-thread execution order counters."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    def test_global_order_increments_across_threads(self) -> None:
        registry = FunWatchRegistry.instance()
        g1, _t1 = registry.next_execution_order("rt1", 100)
        g2, _t2 = registry.next_execution_order("rt1", 200)
        g3, _t3 = registry.next_execution_order("rt1", 100)
        g4, _t4 = registry.next_execution_order("rt1", 200)
        assert [g1, g2, g3, g4] == [1, 2, 3, 4]

    def test_thread_order_independent_per_thread(self) -> None:
        registry = FunWatchRegistry.instance()
        _g1, t1 = registry.next_execution_order("rt1", 100)
        _g2, t2 = registry.next_execution_order("rt1", 200)
        _g3, t3 = registry.next_execution_order("rt1", 100)
        _g4, t4 = registry.next_execution_order("rt1", 200)
        assert [t1, t3] == [1, 2]
        assert [t2, t4] == [1, 2]

    def test_single_thread_both_orders_equal(self) -> None:
        registry = FunWatchRegistry.instance()
        g1, t1 = registry.next_execution_order("rt1", 100)
        g2, t2 = registry.next_execution_order("rt1", 100)
        assert g1 == t1 == 1
        assert g2 == t2 == 2


class TestLifecycleLogFields:
    """Verify lifecycle logs include log_id and execution_order for correlation."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()
        structlog.contextvars.clear_contextvars()

    def teardown_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=42)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_started_log_includes_log_id_and_execution_order(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch
            def process(self) -> None:
                pass

        app = App()
        app.process()
        started_call = mock_logger.log.call_args_list[0]
        kwargs = started_call[1]
        assert kwargs["log_id"] == 42
        assert kwargs["execution_order"] == 1
        assert kwargs["log_role"] == "single"

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=42)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_completed_log_includes_log_id(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch
            def process(self) -> None:
                pass

        app = App()
        app.process()
        completed_call = mock_logger.log.call_args_list[1]
        kwargs = completed_call[1]
        assert kwargs["log_id"] == 42

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=42)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_exception_log_includes_log_id(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_start_log: MagicMock,
        _mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch
            def process(self) -> None:
                raise ValueError("test")

        app = App()
        with pytest.raises(ValueError):
            app.process()
        exception_call = mock_logger.exception.call_args
        kwargs = exception_call[1]
        assert kwargs["log_id"] == 42


class TestModulePathResolution:
    """Test that module_path resolves to instance class for inherited methods."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_own_method_uses_definition_file(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        mock_logger = MagicMock()

        class App(FunWatchMixin):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

            @fun_watch
            def do_work(self) -> None:
                pass

        app = App()
        app.do_work()

        log_call = mock_logger.log.call_args
        assert log_call is not None
        assert log_call[1]["module_name"] == "test_decorator.py"
        assert "test_decorator.py" in log_call[1]["module_path"]

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_inherited_method_resolves_to_instance_class_module(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        """When a @fun_watch method is inherited, module_path resolves to the subclass module."""
        mock_logger = MagicMock()

        class Base(FunWatchMixin):
            @fun_watch
            def inherited_work(self) -> None:
                pass

        class Child(Base):
            app_id = "test_app"
            runtime = "test_runtime"
            logger = mock_logger

        # Simulate cross-module inheritance by patching the decorated func's __module__
        original_method = Base.__dict__["inherited_work"]
        original_module = original_method.__module__
        original_method.__module__ = "data_collector.scraping.threaded"
        try:
            app = Child()
            app.inherited_work()

            log_call = mock_logger.log.call_args
            assert log_call is not None
            # module_path should resolve to the test file (Child's module), not threaded
            assert log_call[1]["module_name"] == "test_decorator.py"
            assert "test_decorator.py" in log_call[1]["module_path"]
        finally:
            original_method.__module__ = original_module
