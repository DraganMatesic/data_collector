"""Thread safety tests for @fun_watch decorator."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock, patch

import pytest

from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch

_REGISTRY = "data_collector.utilities.fun_watch.FunWatchRegistry"


class ThreadApp:
    def __init__(self) -> None:
        self.app_id = "thread_test_app"
        self.runtime = "thread_test_runtime"

    @fun_watch
    def worker(self, items: list[int]) -> int:
        total = 0
        for item in items:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
            total += item
        return total

    @fun_watch
    def simple(self) -> None:
        pass


class SharedInstanceRaceApp:
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
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
            time.sleep(0.0001)
        return count


class FanOutApp:
    def __init__(self) -> None:
        self.app_id = "fan_out_app"
        self.runtime = "fan_out_runtime"

    @fun_watch
    def run_with_wrapper(self, worker_count: int) -> int:
        registry = FunWatchRegistry.instance()

        def worker() -> None:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]

        wrapped_worker = registry.wrap_with_active_context(worker)
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(wrapped_worker) for _ in range(worker_count)]
            for future in futures:
                future.result()
        return worker_count

    @fun_watch
    def run_without_wrapper(self, worker_count: int) -> int:
        def worker() -> None:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker) for _ in range(worker_count)]
            for future in futures:
                future.result()
        return worker_count


class TestConcurrentCalls:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_independent_solved_counters(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
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
        assert mock_insert_log.call_count == 8

        for call in mock_insert_log.call_args_list:
            assert call[1]["solved"] == 10

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_each_call_has_thread_id(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = ThreadApp()

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(app.worker, [1]) for _ in range(4)]
            for f in futures:
                f.result()

        thread_ids = {call[1]["thread_id"] for call in mock_insert_log.call_args_list}
        assert len(thread_ids) >= 1

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_shared_instance_context_isolation_under_overlap(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = SharedInstanceRaceApp()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(app.worker, 1, 0.0),
                executor.submit(app.worker, 80, 0.002),
            ]
            results = [future.result() for future in futures]

        assert sorted(results) == [1, 80]
        assert mock_insert_log.call_count == 2

        solved_values = sorted(call[1]["solved"] for call in mock_insert_log.call_args_list)
        assert solved_values == [1, 80]

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_child_threads_inherit_parent_context_via_wrapper(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FanOutApp()

        result = app.run_with_wrapper(12)

        assert result == 12
        assert mock_insert_log.call_count == 1
        log_kwargs = mock_insert_log.call_args[1]
        assert log_kwargs["solved"] == 12
        assert log_kwargs["failed"] == 0

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_child_thread_without_wrapper_raises_runtime_error(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = FanOutApp()

        with pytest.raises(RuntimeError, match="FunWatchContext is not active"):
            app.run_without_wrapper(4)


class TestExecutionOrderSequencing:
    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    @patch(f"{_REGISTRY}.insert_function_log")
    @patch(f"{_REGISTRY}.update_last_seen")
    @patch(f"{_REGISTRY}.register_function")
    def test_sequential_calls_increment_execution_order(
        self,
        mock_register: MagicMock,
        mock_last_seen: MagicMock,
        mock_insert_log: MagicMock,
    ) -> None:
        app = ThreadApp()
        app.simple()
        app.simple()
        app.simple()

        execution_orders = [
            call[1]["execution_order"]
            for call in mock_insert_log.call_args_list
        ]
        assert execution_orders == [1, 2, 3]
