"""Multi-threaded @fun_watch usage with independent per-thread counters.

Demonstrates:
    - ThreadPoolExecutor with multiple workers calling the same decorated method
    - Each thread gets its own FunWatchContext (closure-local, no cross-talk)
    - execution_order increments per (runtime, thread_id) pair
    - Thread-safe solved counters (no data corruption)

Output shows ORM repr() of each FunctionLog row, followed by a
per-thread summary table.

Run:
    python -m data_collector.examples run fun_watch/03_multithreaded
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from unittest.mock import patch

from data_collector.tables.log import FunctionLog
from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals


class BatchProcessor:
    """Application that processes batches across multiple threads."""

    def __init__(self) -> None:
        self.app_id = "batch_processor_hash"
        self.runtime = "runtime_mt_demo"

    @fun_watch
    def process_batch(self, items: list[int]) -> int:
        """Process a batch of integers, marking each as solved."""
        total = 0
        for item in items:
            self._fw_ctx.mark_solved()  # type: ignore[attr-defined]
            total += item
        return total


def main() -> None:
    """Run multi-threaded @fun_watch example showing thread isolation."""
    FunWatchRegistry.reset()
    registry_path = "data_collector.utilities.fun_watch.FunWatchRegistry"

    log_rows: list[FunctionLog] = []
    lock = threading.Lock()

    def capture_log(**kwargs: Any) -> None:
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
        with lock:
            log_rows.append(row)

    with (
        patch(f"{registry_path}.register_function"),
        patch(f"{registry_path}.update_last_seen"),
        patch(f"{registry_path}.insert_function_log", side_effect=capture_log),
    ):
        app = BatchProcessor()

        # --- Launch 4 threads, each processing a different batch ---
        print("=== Launching 4 threads with different batch sizes ===")
        batches: list[list[int]] = [
            list(range(10)),
            list(range(20)),
            list(range(15)),
            list(range(5)),
        ]

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(app.process_batch, batch): i
                for i, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                worker_idx = futures[future]
                result = future.result()
                print(f"  Worker {worker_idx}: batch_size={len(batches[worker_idx])}, sum={result}")

        # --- ORM repr of each row ---
        print(f"\n=== {len(log_rows)} FunctionLog rows recorded ===")
        for row in log_rows:
            print(f"  {row!r}")

        # --- Summary table ---
        print("\n=== Per-thread summary ===")
        print(f"  {'thread_id':>12}  {'execution_order':>12}  {'task_size':>10}  {'solved':>7}")
        print(f"  {'-' * 12}  {'-' * 12}  {'-' * 10}  {'-' * 7}")
        for row in sorted(log_rows, key=lambda r: (r.thread_id or 0, r.execution_order or 0)):
            print(
                f"  {row.thread_id or '':>12}  "
                f"{row.execution_order or '':>12}  "
                f"{row.task_size or '':>10}  "
                f"{row.solved or '':>7}"
            )

        # --- Verify no cross-talk ---
        print("\n=== Thread isolation verification ===")
        expected_solved = {len(b) for b in batches}
        actual_solved = {row.solved for row in log_rows}
        print(f"  Expected solved values: {sorted(expected_solved)}")
        print(f"  Actual solved values  : {sorted(actual_solved)}")

        distinct_threads = {row.thread_id for row in log_rows}
        print(f"  Distinct thread IDs   : {len(distinct_threads)}")

        if actual_solved == expected_solved:
            print("  Result: No cross-talk detected -- each thread tracked independently")
        else:
            print("  Result: UNEXPECTED -- solved values do not match batch sizes")

    FunWatchRegistry.reset()


if __name__ == "__main__":
    main()
