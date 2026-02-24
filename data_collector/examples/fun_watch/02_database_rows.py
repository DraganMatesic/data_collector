"""Database row structure produced by @fun_watch.

Demonstrates:
    - AppFunctions registration row (function_hash, function_name, filepath, app_id)
    - FunctionLog row fields (execution_order, task_size, solved, failed, timing)
    - task_size auto-detection from first arg with __len__
    - task_size=None when first arg has no __len__ or no args at all
    - main_app defaults to app_id when self.main_app is not set

Output shows actual ORM repr() of each row, identical to what you see
in a debugger or logging session.

Run:
    python -m data_collector.examples run fun_watch/02_database_rows
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from data_collector.tables.apps import AppFunctions
from data_collector.tables.log import FunctionLog
from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals


class Collector:
    """Example app with optional main_app override."""

    def __init__(self, app_id: str, runtime: str, main_app: str = "") -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.main_app = main_app

    @fun_watch
    def fetch_pages(self, urls: list[str]) -> int:
        """Fetch a list of URLs (task_size = len(urls))."""
        for _url in urls:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
        return len(urls)

    @fun_watch
    def compute_score(self, value: float) -> float:
        """Compute on a single float (no __len__ -> task_size=None)."""
        self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
        return value * 2.0

    @fun_watch
    def heartbeat(self) -> str:
        """No arguments at all (task_size=None)."""
        return "alive"


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
    """Run database row structure examples for @fun_watch."""
    FunWatchRegistry.reset()
    registry_path = "data_collector.utilities.fun_watch.FunWatchRegistry"

    with (
        patch(f"{registry_path}.register_function", side_effect=_print_registration),
        patch(f"{registry_path}.update_last_seen"),
        patch(f"{registry_path}.insert_function_log", side_effect=_print_log),
    ):
        # --- task_size from list ---
        print("=== task_size detected from list (len=3) ===")
        app = Collector(app_id="scraper_cro_01", runtime="rt_abc")
        app.fetch_pages(["https://a.com", "https://b.com", "https://c.com"])

        # --- task_size=None for non-sized arg ---
        print("\n=== task_size=None for float argument ===")
        app.compute_score(42.5)

        # --- task_size=None for no args ---
        print("\n=== task_size=None when no arguments ===")
        app.heartbeat()

        # --- main_app defaults to app_id ---
        print("\n=== main_app defaults to app_id ===")
        child = Collector(app_id="child_app", runtime="rt_xyz")
        child.fetch_pages(["https://x.com"])

        # --- main_app explicit override ---
        print("\n=== main_app explicit override ===")
        child2 = Collector(app_id="child_app", runtime="rt_xyz", main_app="root_orchestrator")
        child2.fetch_pages(["https://y.com"])

    FunWatchRegistry.reset()


if __name__ == "__main__":
    main()
