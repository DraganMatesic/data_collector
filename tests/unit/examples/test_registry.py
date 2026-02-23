"""Tests for dynamic examples discovery and selection."""

from pathlib import Path

from data_collector.examples.registry import (
    ExampleEntry,
    _extract_title,
    _has_top_level_main,
    _is_candidate,
    discover_examples,
    filter_by_scope,
    resolve_target,
)


def _write(path: Path, content: str) -> None:
    """Create file content for discovery tests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_is_candidate_filters_internal_files(tmp_path: Path) -> None:
    """Discovery must reject internal and cache files."""
    valid = tmp_path / "request" / "01_valid.py"
    init_file = tmp_path / "request" / "__init__.py"
    main_file = tmp_path / "request" / "__main__.py"
    private_file = tmp_path / "request" / "_helper.py"
    cached_file = tmp_path / "request" / "__pycache__" / "cached.py"

    for path in [valid, init_file, main_file, private_file, cached_file]:
        _write(path, "def main() -> None:\n    return None\n")

    assert _is_candidate(valid) is True
    assert _is_candidate(init_file) is False
    assert _is_candidate(main_file) is False
    assert _is_candidate(private_file) is False
    assert _is_candidate(cached_file) is False


def test_has_top_level_main_detects_sync_and_async(tmp_path: Path) -> None:
    """Only top-level sync/async main should be treated as runnable."""
    sync_file = tmp_path / "sync.py"
    async_file = tmp_path / "async.py"
    no_main_file = tmp_path / "no_main.py"
    syntax_error_file = tmp_path / "broken.py"

    _write(sync_file, "def main() -> None:\n    return None\n")
    _write(async_file, "async def main() -> None:\n    return None\n")
    _write(no_main_file, "def helper() -> None:\n    return None\n")
    _write(syntax_error_file, "def main(:\n    pass\n")

    assert _has_top_level_main(sync_file) is True
    assert _has_top_level_main(async_file) is True
    assert _has_top_level_main(no_main_file) is False
    assert _has_top_level_main(syntax_error_file) is False


def test_discover_examples_recursively_and_extract_title(tmp_path: Path) -> None:
    """Discovery should recurse directories and extract display titles."""
    _write(
        tmp_path / "request" / "01_basic.py",
        '"""Basic request example."""\n\ndef main() -> None:\n    return None\n',
    )
    _write(
        tmp_path / "database" / "postgres" / "01_conn.py",
        '"""Database connection example.\n\nSecond line."""\n\nasync def main() -> None:\n    return None\n',
    )
    _write(tmp_path / "request" / "ignore_me.py", "def helper() -> None:\n    return None\n")
    _write(tmp_path / "request" / "_private.py", "def main() -> None:\n    return None\n")

    entries = discover_examples(tmp_path)
    refs = [entry.ref for entry in entries]

    assert refs == ["database/postgres/01_conn", "request/01_basic"]

    by_ref = {entry.ref: entry for entry in entries}
    assert by_ref["request/01_basic"].group == "request"
    assert by_ref["database/postgres/01_conn"].group == "database/postgres"
    assert by_ref["request/01_basic"].module == "data_collector.examples.request.01_basic"
    assert by_ref["database/postgres/01_conn"].title == "Database connection example."


def test_extract_title_fallback_to_stem(tmp_path: Path) -> None:
    """When docstring is missing, title should fallback to stem."""
    target = tmp_path / "request" / "plain_example.py"
    _write(target, "def main() -> None:\n    return None\n")
    assert _extract_title(target) == "plain_example"


def test_filter_by_scope_and_resolve_target() -> None:
    """Scope and target resolution should handle exact, scoped, and global refs."""
    entries = [
        ExampleEntry(
            ref="database/postgres/01_conn",
            group="database/postgres",
            module="data_collector.examples.database.postgres.01_conn",
            path=Path("database/postgres/01_conn.py"),
            title="conn",
        ),
        ExampleEntry(
            ref="database/01_seed",
            group="database",
            module="data_collector.examples.database.01_seed",
            path=Path("database/01_seed.py"),
            title="seed",
        ),
        ExampleEntry(
            ref="request/01_basic",
            group="request",
            module="data_collector.examples.request.01_basic",
            path=Path("request/01_basic.py"),
            title="basic",
        ),
    ]

    scoped = filter_by_scope(entries, "database")
    assert [entry.ref for entry in scoped] == ["database/01_seed", "database/postgres/01_conn"]

    all_targets = resolve_target(entries, "all")
    assert [entry.ref for entry in all_targets] == [entry.ref for entry in entries]

    scoped_targets = resolve_target(entries, "database/all")
    assert [entry.ref for entry in scoped_targets] == ["database/01_seed", "database/postgres/01_conn"]

    single_target = resolve_target(entries, "request/01_basic")
    assert [entry.ref for entry in single_target] == ["request/01_basic"]

    assert resolve_target(entries, "request/missing") == []
