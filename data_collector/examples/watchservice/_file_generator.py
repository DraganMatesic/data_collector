"""Shared file generation helper for WatchService examples.

Not discovered as an example (underscore prefix).  Imported by
01_stress_test.py and 02_crash_recovery.py.
"""

from __future__ import annotations

import os
import random
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

MIN_FILE_SIZE = 10_240          # 10 KB
MAX_FILE_SIZE = 5_242_880       # 5 MB
DEFAULT_WRITER_THREADS = 8


def write_single_file(index: int, target_dir: Path) -> None:
    """Write a single random .bin file to disk."""
    file_size = random.randint(MIN_FILE_SIZE, MAX_FILE_SIZE)
    file_name = f"file_{index:04d}.bin"
    file_path = target_dir / file_name
    file_path.write_bytes(os.urandom(file_size))


def generate_files(
    total: int,
    root_a: Path,
    root_b: Path,
    *,
    start_index: int = 0,
    stop_event: threading.Event | None = None,
    writer_threads: int = DEFAULT_WRITER_THREADS,
    progress_interval: int = 100,
) -> dict[str, int]:
    """Generate files across two watch roots using a thread pool.

    Args:
        total: Number of files to generate.
        root_a: First watch root directory.
        root_b: Second watch root directory.
        start_index: Starting file index (for continuation after crash).
        stop_event: Optional event to abort generation early.
        writer_threads: Number of concurrent writer threads.
        progress_interval: Print progress every N completed files.

    Returns:
        Counts per root directory (keys: "root_a", "root_b").
    """
    counts = {"root_a": 0, "root_b": 0}
    counter_lock = threading.Lock()
    paths = [
        (root_a, "root_a"),
        (root_b, "root_b"),
    ]

    completed_count = 0

    def _on_file_written(counter_key: str) -> None:
        with counter_lock:
            counts[counter_key] += 1

    with ThreadPoolExecutor(max_workers=writer_threads, thread_name_prefix="file-writer") as executor:
        futures: list[Future[None]] = []
        for offset in range(total):
            if stop_event is not None and stop_event.is_set():
                break
            index = start_index + offset
            target_dir, key = paths[offset % 2]

            def _write_and_count(directory: Path, file_index: int, counter_key: str) -> None:
                write_single_file(file_index, directory)
                _on_file_written(counter_key)

            future = executor.submit(_write_and_count, target_dir, index, key)
            futures.append(future)

        for future in as_completed(futures):
            future.result()
            completed_count += 1
            if progress_interval > 0 and completed_count % progress_interval == 0:
                filled = int(20 * completed_count / total)
                bar = "=" * filled + "-" * (20 - filled)
                with counter_lock:
                    snapshot_a = counts["root_a"]
                    snapshot_b = counts["root_b"]
                print(
                    f"\r  [{bar}] {completed_count:>4}/{total}"
                    f"  (root_a: {snapshot_a}, root_b: {snapshot_b})",
                    end="", flush=True,
                )

    print()
    return counts
