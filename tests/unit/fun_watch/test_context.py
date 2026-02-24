"""Unit tests for FunWatchContext."""

from __future__ import annotations

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
