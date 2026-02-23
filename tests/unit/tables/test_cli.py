import sys
from unittest.mock import MagicMock, patch

import pytest

from data_collector.tables.__main__ import main

_MAIN_MODULE = "data_collector.tables.__main__"


@patch(f"{_MAIN_MODULE}.Deploy")
def test_cli_populate_exits_0_on_success(
    mock_deploy_cls: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_deploy_cls.return_value.populate_tables.return_value = True
    with patch.object(sys, "argv", ["prog", "populate"]):
        main()
    captured = capsys.readouterr()
    assert "seeded successfully" in captured.out


@patch(f"{_MAIN_MODULE}.Deploy")
def test_cli_populate_exits_1_on_failure(
    mock_deploy_cls: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_deploy_cls.return_value.populate_tables.return_value = False
    with patch.object(sys, "argv", ["prog", "populate"]), pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "errors" in captured.err


@patch(f"{_MAIN_MODULE}.Deploy")
def test_cli_setup_exits_1_on_populate_failure(
    mock_deploy_cls: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_deploy_cls.return_value.populate_tables.return_value = False
    with patch.object(sys, "argv", ["prog", "setup"]), pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "errors" in captured.err
