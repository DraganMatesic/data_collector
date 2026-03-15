"""Unit tests for scaffold CLI entry point."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from data_collector.scaffold.__main__ import main

_MODULE = "data_collector.scaffold.__main__"


@patch(f"{_MODULE}.scaffold_app")
def test_cli_create_dispatches_to_scaffold_app(
    mock_scaffold: MagicMock,
) -> None:
    with patch.object(sys, "argv", ["prog", "create", "--group", "cro", "--parent", "fin", "--name", "test_app"]):
        main()
    mock_scaffold.assert_called_once_with(group="cro", parent="fin", name="test_app", app_type="single")


@patch(f"{_MODULE}.scaffold_app")
def test_cli_create_threaded_type(
    mock_scaffold: MagicMock,
) -> None:
    argv = ["prog", "create", "--group", "cro", "--parent", "fin", "--name", "app", "--type", "threaded"]
    with patch.object(sys, "argv", argv):
        main()
    mock_scaffold.assert_called_once_with(group="cro", parent="fin", name="app", app_type="threaded")


@patch(f"{_MODULE}.scaffold_app")
def test_cli_create_default_type_is_single(
    mock_scaffold: MagicMock,
) -> None:
    with patch.object(sys, "argv", ["prog", "create", "--group", "x", "--parent", "y", "--name", "z"]):
        main()
    assert mock_scaffold.call_args.kwargs["app_type"] == "single"


@patch(f"{_MODULE}.scaffold_app")
def test_cli_create_async_type(
    mock_scaffold: MagicMock,
) -> None:
    argv = ["prog", "create", "--group", "cro", "--parent", "fin", "--name", "app", "--type", "async"]
    with patch.object(sys, "argv", argv):
        main()
    mock_scaffold.assert_called_once_with(group="cro", parent="fin", name="app", app_type="async")


@patch(f"{_MODULE}.scaffold_app")
def test_cli_create_dramatiq_type(
    mock_scaffold: MagicMock,
) -> None:
    argv = ["prog", "create", "--group", "cro", "--parent", "fin", "--name", "ocr", "--type", "dramatiq"]
    with patch.object(sys, "argv", argv):
        main()
    mock_scaffold.assert_called_once_with(group="cro", parent="fin", name="ocr", app_type="dramatiq")


def test_cli_missing_required_args() -> None:
    with patch.object(sys, "argv", ["prog", "create", "--group", "x"]), pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2


@patch(f"{_MODULE}.enable_app")
def test_cli_enable(mock_enable: MagicMock) -> None:
    with patch.object(sys, "argv", ["prog", "enable", "--group", "cro", "--parent", "fin", "--name", "app"]):
        main()
    mock_enable.assert_called_once_with(group="cro", parent="fin", name="app")


@patch(f"{_MODULE}.disable_app")
def test_cli_disable(mock_disable: MagicMock) -> None:
    with patch.object(sys, "argv", ["prog", "disable", "--group", "cro", "--parent", "fin", "--name", "app"]):
        main()
    mock_disable.assert_called_once_with(group="cro", parent="fin", name="app")


@patch(f"{_MODULE}.unmanage_app")
def test_cli_unmanage(mock_unmanage: MagicMock) -> None:
    with patch.object(sys, "argv", ["prog", "unmanage", "--group", "cro", "--parent", "fin", "--name", "app"]):
        main()
    mock_unmanage.assert_called_once_with(group="cro", parent="fin", name="app")


@patch(f"{_MODULE}.remove_app")
def test_cli_remove(mock_remove: MagicMock) -> None:
    with patch.object(sys, "argv", ["prog", "remove", "--group", "cro", "--parent", "fin", "--name", "app"]):
        main()
    mock_remove.assert_called_once_with(group="cro", parent="fin", name="app", grace_days=30)


@patch(f"{_MODULE}.remove_app")
def test_cli_remove_custom_grace_days(mock_remove: MagicMock) -> None:
    argv = ["prog", "remove", "--group", "cro", "--parent", "fin", "--name", "app", "--grace-days", "7"]
    with patch.object(sys, "argv", argv):
        main()
    mock_remove.assert_called_once_with(group="cro", parent="fin", name="app", grace_days=7)


def test_cli_no_command_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    with patch.object(sys, "argv", ["prog"]):
        main()
    captured = capsys.readouterr()
    assert "Available commands" in captured.out or "usage" in captured.out
