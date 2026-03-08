"""Thread safety tests for @fun_watch decorator."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock, patch

import pytest
import structlog  # type: ignore[import-untyped]

from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry, fun_watch

_REGISTRY = "data_collector.utilities.fun_watch.FunWatchRegistry"


class ThreadApp(FunWatchMixin):
    def __init__(self) -> None:
        self.app_id = "thread_test_app"
        self.runtime = "thread_test_runtime"

    @fun_watch
    def worker(self, items: list[int]) -> int:
        total = 0
        for item in items:
            self._fun_watch.mark_solved()
            total += item
        return total

    @fun_watch
    def simple(self) -> None:
        pass


class SharedInstanceRaceApp(FunWatchMixin):
    def __init__(self) -> None:
        self.app_id = "shared_instance_race_app"
        self.runtime = "shared_instance_race_runtime"
        self.start_barrier = threading.Barrier(2)

    @fun_watch
    def worker(self, count: int, initial_delay: float = 0.0) -> int:
        self.start_barrier.wait()
        if initial_delay > 0:
            time.sleep(initial_delay)
        for _ in range(count):
            self._fun_watch.mark_solved()
            time.sleep(0.0001)
        return count


class FanOutApp(FunWatchMixin):
    def __init__(self) -> None:
        self.app_id = "fan_out_app"
        self.runtime = "fan_out_runtime"

    @fun_watch
    def run_with_wrapper(self, worker_count: int) -> int:
        registry = FunWatchRegistry.instance()

        def worker() -> None:
            self._fun_watch.mark_solved()

        wrapped_worker = registry.wrap_with_active_context(worker)
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(wrapped_worker) for _ in range(worker_count)]
            for future in futures:
                future.result()
        return worker_count

    @fun_watch
    def run_without_wrapper(self, worker_count: int) -> int:
        def worker() -> None:
            self._fun_watch.mark_solved()

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker) for _ in range(worker_count)]
            for future in futures:
                future.result()
        return worker_count


class FanOutNestedWorkerApp(FunWatchMixin):
    def __init__(self) -> None:
        self.app_id = "fan_out_nested_worker_app"
        self.runtime = "fan_out_nested_worker_runtime"

    @fun_watch
    def inner(self) -> None:
        self._fun_watch.mark_solved(2)

    @fun_watch
    def run_worker_nested_update(self) -> int:
        registry = FunWatchRegistry.instance()

        def worker() -> None:
            self.inner()
            self._fun_watch.mark_solved()

        wrapped_worker = registry.wrap_with_active_context(worker)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(wrapped_worker)
            future.result()
        return 3


class FanOutCleanupApp(FunWatchMixin):
    def __init__(self) -> None:
        self.app_id = "fan_out_cleanup_app"
        self.runtime = "fan_out_cleanup_runtime"

    @fun_watch
    def run_wrapped_then_raw_on_reused_thread(self) -> tuple[str, str]:
        registry = FunWatchRegistry.instance()

        def failing_worker() -> None:
            self._fun_watch.mark_solved()
            raise ValueError("worker failure")

        def raw_worker() -> None:
            self._fun_watch.mark_solved()

        wrapped_failing_worker = registry.wrap_with_active_context(failing_worker)

        with ThreadPoolExecutor(max_workers=1) as executor:
            first_future = executor.submit(wrapped_failing_worker)
            try:
                first_future.result()
            except Exception as exc:
                first_error = type(exc).__name__
            else:
                first_error = "None"

            second_future = executor.submit(raw_worker)
            try:
                second_future.result()
            except Exception as exc:
                second_error = type(exc).__name__
            else:
                second_error = "None"

        return first_error, second_error


class TestConcurrentCalls:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_independent_solved_counters(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = ThreadApp()
        results: list[int] = []

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(app.worker, list(range(10)))
                for _ in range(8)
            ]
            for future in as_completed(futures):
                results.append(future.result())

        assert len(results) == 8
        assert mock_complete_log.call_count == 8

        for call in mock_complete_log.call_args_list:
            assert call[1]["solved"] == 10

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_each_call_has_thread_id(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = ThreadApp()

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(app.worker, [1]) for _ in range(4)]
            for f in futures:
                f.result()

        thread_ids = {call[1]["thread_id"] for call in mock_start_log.call_args_list}
        assert len(thread_ids) >= 1

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_shared_instance_context_isolation_under_overlap(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = SharedInstanceRaceApp()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(app.worker, 1, 0.0),
                executor.submit(app.worker, 80, 0.002),
            ]
            results = [future.result() for future in futures]

        assert sorted(results) == [1, 80]
        assert mock_complete_log.call_count == 2

        solved_values = sorted(call[1]["solved"] for call in mock_complete_log.call_args_list)
        assert solved_values == [1, 80]

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_child_threads_inherit_parent_context_via_wrapper(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FanOutApp()

        result = app.run_with_wrapper(12)

        assert result == 12
        assert mock_complete_log.call_count == 1
        log_kwargs = mock_complete_log.call_args[1]
        assert log_kwargs["solved"] == 12
        assert log_kwargs["failed"] == 0

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_wrapped_worker_keeps_parent_context_after_nested_invocation(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FanOutNestedWorkerApp()

        result = app.run_worker_nested_update()

        assert result == 3
        assert mock_complete_log.call_count == 2

        rows = [call[1] for call in mock_complete_log.call_args_list]
        assert any(row["solved"] == 2 and row["failed"] == 0 for row in rows)
        assert any(row["solved"] == 1 and row["failed"] == 0 for row in rows)

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_wrap_with_active_context_unbinds_on_worker_exception(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FanOutCleanupApp()

        first_error, second_error = app.run_wrapped_then_raw_on_reused_thread()

        assert first_error == "ValueError"
        assert second_error == "RuntimeError"
        assert mock_complete_log.call_count == 1

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_wrap_propagates_structlog_contextvars_to_worker_thread(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        captured: dict[str, object] = {}

        class CtxVarApp(FunWatchMixin):
            app_id = "ctxvar_test_app"
            runtime = "ctxvar_test_runtime"

            @fun_watch
            def parent_method(self) -> None:
                registry = FunWatchRegistry.instance()

                def worker() -> None:
                    captured.update(structlog.contextvars.get_contextvars())

                wrapped = registry.wrap_with_active_context(worker)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(wrapped).result()

        app = CtxVarApp()
        app.parent_method()

        assert "function_id" in captured
        assert "call_chain" in captured
        assert "thread_id" in captured

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_wrap_cleans_up_structlog_contextvars_after_worker(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        after_cleanup: dict[str, object] = {}

        class CleanupApp(FunWatchMixin):
            app_id = "cleanup_test_app"
            runtime = "cleanup_test_runtime"

            @fun_watch
            def parent_method(self) -> None:
                registry = FunWatchRegistry.instance()

                def worker() -> None:
                    pass

                def checker() -> None:
                    after_cleanup.update(structlog.contextvars.get_contextvars())

                wrapped = registry.wrap_with_active_context(worker)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(wrapped).result()
                    executor.submit(checker).result()

        app = CleanupApp()
        app.parent_method()

        assert "function_id" not in after_cleanup

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_child_thread_without_wrapper_raises_runtime_error(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = FanOutApp()

        with pytest.raises(RuntimeError, match="FunWatchContext is not active"):
            app.run_without_wrapper(4)


class TestExecutionOrderSequencing:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_sequential_calls_increment_execution_order(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        app = ThreadApp()
        app.simple()
        app.simple()
        app.simple()

        execution_orders = [
            call[1]["execution_order"]
            for call in mock_start_log.call_args_list
        ]
        assert execution_orders == [1, 2, 3]

    def test_counter_cleanup_on_runtime_change(self) -> None:
        registry = FunWatchRegistry.instance()
        tid = 1

        assert registry.next_execution_order("runtime_a", tid) == (1, 1)
        assert registry.next_execution_order("runtime_a", tid) == (2, 2)
        assert registry.next_execution_order("runtime_a", tid) == (3, 3)

        assert registry.next_execution_order("runtime_b", tid) == (1, 1)
        assert registry.next_execution_order("runtime_b", tid) == (2, 2)


class TestWrapWithActiveContext:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.update_parent_log_role")
    @patch(f"{_REGISTRY}.complete_function_log")
    @patch(f"{_REGISTRY}.start_function_log", return_value=1)
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_wrap_overrides_thread_id_to_worker_thread(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_start_log: MagicMock,
        mock_complete_log: MagicMock,
        _mock_parent_role: MagicMock,
    ) -> None:
        captured_thread_ids: list[int] = []
        parent_thread_id = threading.get_ident()

        class CtxApp(FunWatchMixin):
            app_id = "ctx_thread_app"
            runtime = "ctx_thread_runtime"

            @fun_watch
            def parent_method(self) -> None:
                registry = FunWatchRegistry.instance()

                def worker() -> None:
                    captured_thread_ids.append(
                        structlog.contextvars.get_contextvars().get("thread_id", 0)
                    )

                wrapped = registry.wrap_with_active_context(worker)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(wrapped).result()

        app = CtxApp()
        app.parent_method()

        assert len(captured_thread_ids) == 1
        worker_thread_id = captured_thread_ids[0]
        assert worker_thread_id != 0
        assert worker_thread_id != parent_thread_id
