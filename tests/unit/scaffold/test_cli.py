"""Unit tests for scaffold CLI entry point."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from data_collector.scaffold.__main__ import main

_MODULE = "data_collector.scaffold.__main__"


@patch(f"{_MODULE}.scaffold_app")
def test_cli_dispatches_to_scaffold_app(
    mock_scaffold: MagicMock,
) -> None:
    with patch.object(sys, "argv", ["prog", "--group", "cro", "--parent", "fin", "--name", "test_app"]):
        main()
    mock_scaffold.assert_called_once_with(group="cro", parent="fin", name="test_app", app_type="single")


@patch(f"{_MODULE}.scaffold_app")
def test_cli_threaded_type(
    mock_scaffold: MagicMock,
) -> None:
    argv = ["prog", "--group", "cro", "--parent", "fin", "--name", "app", "--type", "threaded"]
    with patch.object(sys, "argv", argv):
        main()
    mock_scaffold.assert_called_once_with(group="cro", parent="fin", name="app", app_type="threaded")


@patch(f"{_MODULE}.scaffold_app")
def test_cli_default_type_is_single(
    mock_scaffold: MagicMock,
) -> None:
    with patch.object(sys, "argv", ["prog", "--group", "x", "--parent", "y", "--name", "z"]):
        main()
    assert mock_scaffold.call_args.kwargs["app_type"] == "single"


@patch(f"{_MODULE}.scaffold_app")
def test_cli_async_type(
    mock_scaffold: MagicMock,
) -> None:
    argv = ["prog", "--group", "cro", "--parent", "fin", "--name", "app", "--type", "async"]
    with patch.object(sys, "argv", argv):
        main()
    mock_scaffold.assert_called_once_with(group="cro", parent="fin", name="app", app_type="async")


def test_cli_missing_required_args() -> None:
    with patch.object(sys, "argv", ["prog", "--group", "x"]), pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2
