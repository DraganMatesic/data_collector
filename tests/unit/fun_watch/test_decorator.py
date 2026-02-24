"""Unit tests for @fun_watch decorator behavior."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch

_REGISTRY = "data_collector.utilities.fun_watch.FunWatchRegistry"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeApp:
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
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
        return len(items)

    @fun_watch
    def failing_method(self, items: list[str]) -> None:
        self._fw_ctx.mark_solved(1)  # type: ignore[attr-defined]
        raise ValueError("test error")

    @fun_watch
    def no_args_method(self) -> str:
        return "done"

    @fun_watch
    def non_sized_arg(self, value: int) -> int:
        return value


class NestedApp:
    """App with nested decorated methods to validate context restoration."""

    def __init__(self) -> None:
        self.app_id = "nested_app_hash"
        self.runtime = "nested_runtime_hash"

    @fun_watch
    def inner(self, items: list[str]) -> int:
        for _item in items:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
        return len(items)

    @fun_watch
    def outer(self, outer_items: list[str], inner_items: list[str]) -> int:
        for _item in outer_items:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
        _ = self.inner(inner_items)
        for _item in outer_items:
            self._fw_ctx.mark_failed()  # type: ignore[attr-defined]
        return len(outer_items) + len(inner_items)


class NestedExceptionApp:
    """App with nested decorated methods where inner call raises an exception."""

    def __init__(self) -> None:
        self.app_id = "nested_exception_app_hash"
        self.runtime = "nested_exception_runtime_hash"

    @fun_watch
    def inner_fail(self, items: list[str]) -> None:
        self._fw_ctx.mark_failed()  # type: ignore[attr-defined]
        raise ValueError("nested failure")

    @fun_watch
    def outer_handles_inner_failure(self, outer_items: list[str], inner_items: list[str]) -> int:
        for _item in outer_items:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
        try:
            self.inner_fail(inner_items)
        except ValueError:
            self._fw_ctx.mark_failed(len(inner_items))  # type: ignore[attr-defined]
        return len(outer_items)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRegistration:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_calls_register_on_first_invocation(
        self,
        mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        mock_register.assert_called_once()
        call_args = mock_register.call_args
        assert call_args[0][1] == "process_items"

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_calls_register_on_each_invocation(
        self,
        mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        app.process_items(["b"])
        assert mock_register.call_count == 2


class TestFunctionLog:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_inserts_log_on_success(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b"])
        mock_insert_log.assert_called_once()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_solved_count(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["solved"] == 3

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_task_size(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a", "b", "c"])
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["task_size"] == 3

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_none_for_non_sized_arg(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.non_sized_arg(42)
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["task_size"] is None

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_task_size_none_when_no_args(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.no_args_method()
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["task_size"] is None

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_app_id_and_runtime(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp(app_id="my_app", runtime="my_runtime")
        app.process_items(["x"])
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["app_id"] == "my_app"
        assert call_kwargs["runtime_id"] == "my_runtime"

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_main_app_defaults_to_app_id(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp(app_id="my_app", runtime="rt")
        app.process_items(["x"])
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["main_app"] == "my_app"

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_main_app_from_self(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp(app_id="child", runtime="rt", main_app="root_app")
        app.process_items(["x"])
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["main_app"] == "root_app"


class TestExceptionHandling:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_still_records_log_on_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError, match="test error"):
            app.failing_method(["x"])
        mock_insert_log.assert_called_once()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_records_partial_solved_on_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        call_kwargs = mock_insert_log.call_args[1]
        assert call_kwargs["solved"] == 1

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_re_raises_original_exception(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError, match="test error"):
            app.failing_method(["x"])


class TestReturnValue:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_returns_function_result(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        result = app.process_items(["a", "b"])
        assert result == 2

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_returns_string_result(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        _mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        result = app.no_args_method()
        assert result == "done"


class TestNestedInvocations:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_success_restores_outer_context(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = NestedApp()
        result = app.outer(["a", "b"], ["x", "y", "z"])

        assert result == 5
        assert mock_insert_log.call_count == 2

        rows = [call[1] for call in mock_insert_log.call_args_list]
        assert any(row["task_size"] == 3 and row["solved"] == 3 and row["failed"] == 0 for row in rows)
        assert any(row["task_size"] == 2 and row["solved"] == 2 and row["failed"] == 2 for row in rows)

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_nested_exception_restores_outer_context(
        self,
        _mock_register: MagicMock,
        _mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = NestedExceptionApp()
        result = app.outer_handles_inner_failure(["a", "b"], ["x", "y", "z"])

        assert result == 2
        assert mock_insert_log.call_count == 2

        rows = [call[1] for call in mock_insert_log.call_args_list]
        assert any(row["task_size"] == 3 and row["solved"] == 0 and row["failed"] == 1 for row in rows)
        assert any(row["task_size"] == 2 and row["solved"] == 2 and row["failed"] == 3 for row in rows)


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

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_updates_last_seen_on_success(
        self,
        _mock_register: MagicMock,
        mock_last_seen: MagicMock,
        _mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        app.process_items(["a"])
        mock_last_seen.assert_called_once()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_updates_last_seen_on_exception(
        self,
        _mock_register: MagicMock,
        mock_last_seen: MagicMock,
        _mock_insert_log: MagicMock,
    ) -> None:
        app = FakeApp()
        with pytest.raises(ValueError):
            app.failing_method(["x"])
        mock_last_seen.assert_called_once()
