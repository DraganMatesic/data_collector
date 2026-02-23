"""CLI entry point for running package examples."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
from collections import defaultdict
from collections.abc import Callable, Coroutine, Sequence
from typing import cast

from data_collector.examples.registry import ExampleEntry, discover_examples, filter_by_scope, resolve_target


def _group_label(group: str) -> str:
    """Return user-friendly label for discovered group key."""
    return "<root>" if group == "." else group


def _run_entry(entry: ExampleEntry) -> tuple[bool, str]:
    """Import and execute discovered module `main` function."""
    try:
        module = importlib.import_module(entry.module)
    except Exception as exc:
        return False, f"Import failed: {exc}"

    main_callable = getattr(module, "main", None)
    if not callable(main_callable):
        return False, "No callable main() found."

    try:
        if inspect.iscoroutinefunction(main_callable):
            async_main = cast(Callable[[], Coroutine[object, object, None]], main_callable)
            asyncio.run(async_main())
        else:
            sync_main = cast(Callable[[], None], main_callable)
            sync_main()
    except Exception as exc:
        return False, f"Execution failed: {exc}"

    return True, "ok"


def _handle_list(scope: str | None) -> int:
    """List discovered examples grouped by scope."""
    entries = discover_examples()
    selected = filter_by_scope(entries, scope)

    if not selected:
        if scope:
            print(f"No examples found for scope '{scope}'.")
            return 1
        print("No runnable examples found.")
        return 0

    grouped: dict[str, list[ExampleEntry]] = defaultdict(list)
    for entry in selected:
        grouped[entry.group].append(entry)

    for group_name in sorted(grouped):
        print(f"[{_group_label(group_name)}]")
        for entry in grouped[group_name]:
            print(f"  {entry.ref} - {entry.title}")
    return 0


def _handle_run(target: str) -> int:
    """Resolve target and execute selected examples."""
    entries = discover_examples()
    selected = resolve_target(entries, target)

    if not selected:
        print(f"No examples matched target '{target}'.")
        return 1

    passed = 0
    failed = 0
    multi_run = len(selected) > 1

    for entry in selected:
        print(f"[RUN ] {entry.ref}")
        ok, message = _run_entry(entry)
        if ok:
            passed += 1
            print(f"[ OK ] {entry.ref}")
        else:
            failed += 1
            print(f"[FAIL] {entry.ref}: {message}")

    if multi_run:
        print(f"Summary: passed={passed}, failed={failed}, total={len(selected)}")

    return 0 if failed == 0 else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and dispatch examples commands."""
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.examples",
        description="Discover and run packaged examples",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_list = subparsers.add_parser("list", help="List discovered examples")
    parser_list.add_argument(
        "scope",
        nargs="?",
        default=None,
        help="Optional group scope (e.g. request, database, database/postgres)",
    )

    parser_run = subparsers.add_parser("run", help="Run one, scoped, or all examples")
    parser_run.add_argument(
        "target",
        help="Target ref: all | <scope>/all | <group>/<example_name>",
    )

    args = parser.parse_args(argv)

    if args.command == "list":
        return _handle_list(cast(str | None, args.scope))
    if args.command == "run":
        return _handle_run(cast(str, args.target))

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
