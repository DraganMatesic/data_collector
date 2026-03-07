"""Unit tests for FunWatchContext."""

from __future__ import annotations

import json
import threading

from data_collector.utilities.fun_watch import FunWatchContext


class TestFunWatchContext:
    def test_initial_state_with_task_size(self) -> None:
        context = FunWatchContext(task_size=100)
        assert context.solved == 0
        assert context.failed == 0
        assert context.task_size == 100

    def test_initial_state_no_task_size(self) -> None:
        context = FunWatchContext()
        assert context.task_size is None

    def test_mark_solved_default(self) -> None:
        context = FunWatchContext()
        context.mark_solved()
        assert context.solved == 1

    def test_mark_solved_batch(self) -> None:
        context = FunWatchContext()
        context.mark_solved(10)
        assert context.solved == 10

    def test_mark_solved_accumulates(self) -> None:
        context = FunWatchContext()
        context.mark_solved(3)
        context.mark_solved(7)
        assert context.solved == 10

    def test_mark_failed_default(self) -> None:
        context = FunWatchContext()
        context.mark_failed()
        assert context.failed == 1

    def test_mark_failed_batch(self) -> None:
        context = FunWatchContext()
        context.mark_failed(5)
        assert context.failed == 5

    def test_mark_failed_accumulates(self) -> None:
        context = FunWatchContext()
        context.mark_failed(2)
        context.mark_failed(3)
        assert context.failed == 5

    def test_solved_and_failed_independent(self) -> None:
        context = FunWatchContext()
        context.mark_solved(10)
        context.mark_failed(2)
        assert context.solved == 10
        assert context.failed == 2

    def test_mark_solved_waits_for_counter_lock(self) -> None:
        context = FunWatchContext()
        started = threading.Event()
        completed = threading.Event()

        def worker() -> None:
            started.set()
            context.mark_solved()
            completed.set()

        with context._counter_lock:  # type: ignore[attr-defined]
            thread = threading.Thread(target=worker)
            thread.start()
            assert started.wait(1.0)
            assert not completed.wait(0.05)

        thread.join(1.0)
        assert completed.is_set()
        assert context.solved == 1

    def test_mark_failed_waits_for_counter_lock(self) -> None:
        context = FunWatchContext()
        started = threading.Event()
        completed = threading.Event()

        def worker() -> None:
            started.set()
            context.mark_failed()
            completed.set()

        with context._counter_lock:  # type: ignore[attr-defined]
            thread = threading.Thread(target=worker)
            thread.start()
            assert started.wait(1.0)
            assert not completed.wait(0.05)

        thread.join(1.0)
        assert completed.is_set()
        assert context.failed == 1

    def test_mark_failed_backwards_compatible(self) -> None:
        context = FunWatchContext()
        context.mark_failed(3)
        assert context.failed == 3
        item_error_count, types_json, samples_json = context.error_snapshot()
        assert item_error_count == 0
        assert types_json is None
        assert samples_json is None

    def test_mark_failed_with_error_type_aggregates(self) -> None:
        context = FunWatchContext()
        context.mark_failed(2, error_type="ValueError", error_message="bad value")
        context.mark_failed(3, error_type="KeyError", error_message="missing key")
        context.mark_failed(1, error_type="ValueError", error_message="another bad value")
        assert context.failed == 6
        item_error_count, types_json, samples_json = context.error_snapshot()
        assert item_error_count == 6
        types = json.loads(str(types_json))
        assert types == {"ValueError": 3, "KeyError": 3}
        samples = json.loads(str(samples_json))
        assert samples["ValueError"] == ["bad value", "another bad value"]
        assert samples["KeyError"] == ["missing key"]

    def test_mark_failed_with_error_message_samples_capped(self) -> None:
        context = FunWatchContext()
        for i in range(10):
            context.mark_failed(1, error_type="RuntimeError", error_message=f"msg_{i}")
        item_error_count, _types_json, samples_json = context.error_snapshot()
        assert item_error_count == 10
        samples = json.loads(str(samples_json))
        assert len(samples["RuntimeError"]) == 5

    def test_error_snapshot_empty_when_no_typed_errors(self) -> None:
        context = FunWatchContext()
        context.mark_failed(5)
        item_error_count, types_json, samples_json = context.error_snapshot()
        assert item_error_count == 0
        assert types_json is None
        assert samples_json is None

    def test_mark_failed_error_type_without_message(self) -> None:
        context = FunWatchContext()
        context.mark_failed(2, error_type="TimeoutError")
        item_error_count, types_json, samples_json = context.error_snapshot()
        assert item_error_count == 2
        types = json.loads(str(types_json))
        assert types == {"TimeoutError": 2}
        assert samples_json is None
