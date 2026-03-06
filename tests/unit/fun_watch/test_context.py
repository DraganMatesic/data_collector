"""Unit tests for FunWatchContext."""

from __future__ import annotations

import json
import threading

from data_collector.utilities.fun_watch import FunWatchContext


class TestFunWatchContext:
    def test_initial_state_with_task_size(self) -> None:
        ctx = FunWatchContext(task_size=100)
        assert ctx.solved == 0
        assert ctx.failed == 0
        assert ctx.task_size == 100

    def test_initial_state_no_task_size(self) -> None:
        ctx = FunWatchContext()
        assert ctx.task_size is None

    def test_mark_solved_default(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_solved()
        assert ctx.solved == 1

    def test_mark_solved_batch(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_solved(10)
        assert ctx.solved == 10

    def test_mark_solved_accumulates(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_solved(3)
        ctx.mark_solved(7)
        assert ctx.solved == 10

    def test_mark_failed_default(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_failed()
        assert ctx.failed == 1

    def test_mark_failed_batch(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_failed(5)
        assert ctx.failed == 5

    def test_mark_failed_accumulates(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_failed(2)
        ctx.mark_failed(3)
        assert ctx.failed == 5

    def test_solved_and_failed_independent(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_solved(10)
        ctx.mark_failed(2)
        assert ctx.solved == 10
        assert ctx.failed == 2

    def test_mark_solved_waits_for_counter_lock(self) -> None:
        ctx = FunWatchContext()
        started = threading.Event()
        completed = threading.Event()

        def worker() -> None:
            started.set()
            ctx.mark_solved()
            completed.set()

        with ctx._counter_lock:  # type: ignore[attr-defined]
            thread = threading.Thread(target=worker)
            thread.start()
            assert started.wait(1.0)
            assert not completed.wait(0.05)

        thread.join(1.0)
        assert completed.is_set()
        assert ctx.solved == 1

    def test_mark_failed_waits_for_counter_lock(self) -> None:
        ctx = FunWatchContext()
        started = threading.Event()
        completed = threading.Event()

        def worker() -> None:
            started.set()
            ctx.mark_failed()
            completed.set()

        with ctx._counter_lock:  # type: ignore[attr-defined]
            thread = threading.Thread(target=worker)
            thread.start()
            assert started.wait(1.0)
            assert not completed.wait(0.05)

        thread.join(1.0)
        assert completed.is_set()
        assert ctx.failed == 1

    def test_mark_failed_backwards_compatible(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_failed(3)
        assert ctx.failed == 3
        item_error_count, types_json, samples_json = ctx.error_snapshot()
        assert item_error_count == 0
        assert types_json is None
        assert samples_json is None

    def test_mark_failed_with_error_type_aggregates(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_failed(2, error_type="ValueError", error_message="bad value")
        ctx.mark_failed(3, error_type="KeyError", error_message="missing key")
        ctx.mark_failed(1, error_type="ValueError", error_message="another bad value")
        assert ctx.failed == 6
        item_error_count, types_json, samples_json = ctx.error_snapshot()
        assert item_error_count == 6
        types = json.loads(str(types_json))
        assert types == {"ValueError": 3, "KeyError": 3}
        samples = json.loads(str(samples_json))
        assert samples["ValueError"] == ["bad value", "another bad value"]
        assert samples["KeyError"] == ["missing key"]

    def test_mark_failed_with_error_message_samples_capped(self) -> None:
        ctx = FunWatchContext()
        for i in range(10):
            ctx.mark_failed(1, error_type="RuntimeError", error_message=f"msg_{i}")
        item_error_count, _types_json, samples_json = ctx.error_snapshot()
        assert item_error_count == 10
        samples = json.loads(str(samples_json))
        assert len(samples["RuntimeError"]) == 5

    def test_error_snapshot_empty_when_no_typed_errors(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_failed(5)
        item_error_count, types_json, samples_json = ctx.error_snapshot()
        assert item_error_count == 0
        assert types_json is None
        assert samples_json is None

    def test_mark_failed_error_type_without_message(self) -> None:
        ctx = FunWatchContext()
        ctx.mark_failed(2, error_type="TimeoutError")
        item_error_count, types_json, samples_json = ctx.error_snapshot()
        assert item_error_count == 2
        types = json.loads(str(types_json))
        assert types == {"TimeoutError": 2}
        assert samples_json is None
