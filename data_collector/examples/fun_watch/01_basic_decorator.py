"""Basic @fun_watch decorator usage with solved/failed tracking.

Demonstrates:
    - Minimal class with app_id and runtime attributes
    - @fun_watch on instance methods
    - self._fw_ctx.mark_solved() / mark_failed() inside the method body
    - Return value passthrough (decorator is transparent)
    - Exception handling (decorator records metrics, then re-raises)

Output shows actual ORM repr() of AppFunctions and FunctionLog rows
that would be written to the database.

Run:
    python -m data_collector.examples run fun_watch/01_basic_decorator
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from data_collector.tables.apps import AppFunctions
from data_collector.tables.log import FunctionLog
from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals


class DemoApp:
    """Minimal application class satisfying @fun_watch requirements."""

    def __init__(self) -> None:
        self.app_id = "demo_app_hash_abc123"
        self.runtime = "runtime_xyz_789"

    @fun_watch
    def process_records(self, records: list[str]) -> int:
        """Process a batch of records, marking each as solved."""
        for _record in records:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
        return len(records)

    @fun_watch
    def risky_operation(self, items: list[str]) -> None:
        """Process items but fail halfway through."""
        for i, _item in enumerate(items):
            if i >= 2:
                self._fw_ctx.mark_failed(len(items) - i)  # type: ignore[attr-defined]
                raise RuntimeError("Simulated failure after 2 items")
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]


def _print_registration(function_hash: str, function_name: str, filepath: str, app_id: str) -> None:
    """Build and print an AppFunctions ORM instance."""
    row = AppFunctions(
        function_hash=function_hash,
        function_name=function_name,
        filepath=filepath,
        app_id=app_id,
    )
    print(f"  -> {row!r}")


def _print_log(**kwargs: Any) -> None:
    """Build and print a FunctionLog ORM instance."""
    start = kwargs["start_time"]
    end = kwargs["end_time"]
    row = FunctionLog(
        function_hash=kwargs["function_hash"],
        execution_order=kwargs["execution_order"],
        main_app=kwargs["main_app"],
        app_id=kwargs["app_id"],
        thread_id=kwargs["thread_id"],
        task_size=kwargs["task_size"],
        solved=kwargs["solved"],
        failed=kwargs["failed"],
        start_time=start,
        end_time=end,
        totals=get_totals(start, end),
        totalm=get_totalm(start, end),
        totalh=get_totalh(start, end),
        runtime=kwargs["runtime_id"],
    )
    print(f"  -> {row!r}")


def main() -> None:
    """Run basic @fun_watch examples showing decorator behavior."""
    FunWatchRegistry.reset()
    registry_path = "data_collector.utilities.fun_watch.FunWatchRegistry"

    with (
        patch(f"{registry_path}.register_function", side_effect=_print_registration),
        patch(f"{registry_path}.update_last_seen"),
        patch(f"{registry_path}.insert_function_log", side_effect=_print_log),
    ):
        app = DemoApp()

        # --- Success case ---
        print("=== Success: process 5 records ===")
        result = app.process_records(["a", "b", "c", "d", "e"])
        print(f"  Return value: {result}")

        # --- Return value passthrough ---
        print("\n=== Return value passthrough ===")
        count = app.process_records(["x", "y"])
        print(f"  Decorator returns original result: {count}")

        # --- Exception handling ---
        print("\n=== Exception: failure after 2 items ===")
        try:
            app.risky_operation(["r1", "r2", "r3", "r4", "r5"])
        except RuntimeError as exc:
            print(f"  Exception re-raised: {exc}")

    FunWatchRegistry.reset()


if __name__ == "__main__":
    main()
