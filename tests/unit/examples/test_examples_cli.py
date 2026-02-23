"""Tests for examples CLI execution and output behavior."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

import data_collector.examples.__main__ as examples_cli
from data_collector.examples.registry import ExampleEntry



def _entry(ref: str, group: str, module: str) -> ExampleEntry:
    """Create deterministic example entries used by CLI tests."""
    return ExampleEntry(
        ref=ref,
        group=group,
        module=module,
        path=Path(f"{ref}.py"),
        title=ref,
    )


def test_list_all_groups(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """List command should print all discovered groups and refs."""
    entries = [
        _entry("database/01_seed", "database", "examples.database.seed"),
        _entry("request/01_basic", "request", "examples.request.basic"),
    ]
    monkeypatch.setattr(examples_cli, "discover_examples", lambda: entries)

    exit_code = examples_cli.main(["list"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "[database]" in output
    assert "database/01_seed - database/01_seed" in output
    assert "[request]" in output
    assert "request/01_basic - request/01_basic" in output


def test_list_scope(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """List command should filter output when scope is provided."""
    entries = [
        _entry("database/01_seed", "database", "examples.database.seed"),
        _entry("request/01_basic", "request", "examples.request.basic"),
    ]
    monkeypatch.setattr(examples_cli, "discover_examples", lambda: entries)

    exit_code = examples_cli.main(["list", "request"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "[request]" in output
    assert "request/01_basic - request/01_basic" in output
    assert "[database]" not in output


def test_run_entry_executes_sync_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run helper should execute synchronous module main function."""
    called: list[str] = []

    module = ModuleType("fake_sync")

    def sync_main() -> None:
        called.append("sync")

    module.main = sync_main  # type: ignore[attr-defined]

    def fake_import(_name: str, _package: str | None = None) -> ModuleType:
        return module

    monkeypatch.setattr("importlib.import_module", fake_import)

    ok, _message = examples_cli._run_entry(_entry("request/01_basic", "request", "fake.sync"))
    assert ok is True
    assert called == ["sync"]


def test_run_entry_executes_async_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run helper should execute asynchronous module main function."""
    called: list[str] = []

    module = ModuleType("fake_async")

    async def async_main() -> None:
        called.append("async")

    module.main = async_main  # type: ignore[attr-defined]

    def fake_import(_name: str, _package: str | None = None) -> ModuleType:
        return module

    monkeypatch.setattr("importlib.import_module", fake_import)

    ok, _message = examples_cli._run_entry(_entry("request/05_async", "request", "fake.async"))
    assert ok is True
    assert called == ["async"]


def test_run_entry_missing_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run helper should fail when module has no callable main."""
    module = ModuleType("missing_main")

    def fake_import(_name: str, _package: str | None = None) -> ModuleType:
        return module

    monkeypatch.setattr("importlib.import_module", fake_import)

    ok, message = examples_cli._run_entry(_entry("request/01_basic", "request", "fake.missing"))
    assert ok is False
    assert "No callable main" in message


def test_run_all_continues_and_summarizes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Run-all should continue through failures and print summary."""
    entries = [
        _entry("request/01_basic", "request", "examples.request.basic"),
        _entry("request/02_session", "request", "examples.request.session"),
        _entry("database/01_seed", "database", "examples.database.seed"),
    ]
    monkeypatch.setattr(examples_cli, "discover_examples", lambda: entries)

    results = {
        "request/01_basic": (False, "boom"),
        "request/02_session": (True, "ok"),
        "database/01_seed": (True, "ok"),
    }

    def fake_run_entry(entry: ExampleEntry) -> tuple[bool, str]:
        return results[entry.ref]

    monkeypatch.setattr(examples_cli, "_run_entry", fake_run_entry)

    exit_code = examples_cli.main(["run", "all"])
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "[FAIL] request/01_basic: boom" in output
    assert "[ OK ] request/02_session" in output
    assert "Summary: passed=2, failed=1, total=3" in output


def test_run_invalid_target(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Run command should fail cleanly for an unknown target selector."""
    entries = [_entry("request/01_basic", "request", "examples.request.basic")]
    monkeypatch.setattr(examples_cli, "discover_examples", lambda: entries)

    exit_code = examples_cli.main(["run", "request/missing"])
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "No examples matched target 'request/missing'." in output
