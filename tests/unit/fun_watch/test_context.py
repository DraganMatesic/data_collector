"""Unit tests for FunWatchContext."""

from __future__ import annotations

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

    def test_mark_failed_simple(self) -> None:
        context = FunWatchContext()
        context.mark_failed(3)
        assert context.failed == 3

    def test_call_count_initial(self) -> None:
        context = FunWatchContext()
        assert context.call_count == 0

    def test_increment_call_count(self) -> None:
        context = FunWatchContext()
        context.increment_call_count()
        context.increment_call_count()
        context.increment_call_count()
        assert context.call_count == 3

    def test_first_start_time_initial(self) -> None:
        context = FunWatchContext()
        assert context.first_start_time is None

    def test_invocation_durations_initial(self) -> None:
        context = FunWatchContext()
        total, average, median, minimum, maximum = context.timing_snapshot()
        assert total == 0
        assert average == 0
        assert median == 0
        assert minimum == 0
        assert maximum == 0

    def test_record_invocation_duration(self) -> None:
        context = FunWatchContext()
        context.record_invocation_duration(100.0)
        context.record_invocation_duration(200.0)
        context.record_invocation_duration(300.0)
        total, average, median, minimum, maximum = context.timing_snapshot()
        assert total == 600
        assert average == 200
        assert median == 200
        assert minimum == 100
        assert maximum == 300

    def test_timing_snapshot_single_duration(self) -> None:
        context = FunWatchContext()
        context.record_invocation_duration(150.0)
        total, average, median, minimum, maximum = context.timing_snapshot()
        assert total == 150
        assert average == 150
        assert median == 150
        assert minimum == 150
        assert maximum == 150

    def test_timing_snapshot_thread_safety(self) -> None:
        context = FunWatchContext()
        barrier = threading.Barrier(4)

        def worker() -> None:
            barrier.wait()
            for _ in range(100):
                context.record_invocation_duration(10.0)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5.0)

        total, average, median, minimum, maximum = context.timing_snapshot()
        assert total == 4000
        assert average == 10
        assert median == 10
        assert minimum == 10
        assert maximum == 10
