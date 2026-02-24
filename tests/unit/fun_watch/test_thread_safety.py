"""Thread safety tests for @fun_watch decorator."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock, patch

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
