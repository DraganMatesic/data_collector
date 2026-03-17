"""Tests for WatchService CLI root management."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from data_collector.messaging.__main__ import (
    _activate_root,
    _add_root,
    _deactivate_root,
    _list_roots,
    _remove_root,
)


def _mock_database() -> MagicMock:
    """Create a mock database with session context manager."""
    mock_database = MagicMock()
    mock_session = MagicMock()
    mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
    return mock_database


# ---------------------------------------------------------------------------
# TestAddRoot
# ---------------------------------------------------------------------------


class TestAddRoot:
    """Test add-root CLI command."""

    def test_creates_watch_root_row(self) -> None:
        database = _mock_database()
        session = database.create_session.return_value.__enter__.return_value
        captured: list[object] = []

        def capture_add(row: object) -> None:
            row.id = 1  # type: ignore[attr-defined]
            captured.append(row)

        database.add.side_effect = lambda row, session: capture_add(row)

        arguments = argparse.Namespace(
            path="/ingest/hr/gazette",
            rel_path="gazette",
            country="HR",
            group="ocr",
            worker_path="data_collector.croatia.gazette.ocr.main",
            ext=".pdf,.zip",
            no_recursive=False,
        )

        _add_root(arguments, database)

        assert len(captured) == 1
        row = captured[0]
        assert row.root_path == "/ingest/hr/gazette"  # type: ignore[attr-defined]
        assert row.country == "HR"  # type: ignore[attr-defined]
        assert row.watch_group == "ocr"  # type: ignore[attr-defined]
        assert row.worker_path == "data_collector.croatia.gazette.ocr.main"  # type: ignore[attr-defined]
        assert json.loads(row.extensions) == [".pdf", ".zip"]  # type: ignore[attr-defined]
        assert row.recursive is True  # type: ignore[attr-defined]
        session.commit.assert_called_once()

    def test_no_extensions(self) -> None:
        database = _mock_database()
        captured: list[object] = []

        def capture_add(row: object) -> None:
            row.id = 1  # type: ignore[attr-defined]
            captured.append(row)

        database.add.side_effect = lambda row, session: capture_add(row)

        arguments = argparse.Namespace(
            path="/ingest/hr/gazette",
            rel_path="gazette",
            country="HR",
            group="ocr",
            worker_path="data_collector.croatia.gazette.ocr.main",
            ext=None,
            no_recursive=False,
        )

        _add_root(arguments, database)

        assert captured[0].extensions is None  # type: ignore[attr-defined]

    def test_no_recursive_flag(self) -> None:
        database = _mock_database()
        captured: list[object] = []

        def capture_add(row: object) -> None:
            row.id = 1  # type: ignore[attr-defined]
            captured.append(row)

        database.add.side_effect = lambda row, session: capture_add(row)

        arguments = argparse.Namespace(
            path="/ingest/hr/gazette",
            rel_path="gazette",
            country="HR",
            group="ocr",
            worker_path="data_collector.croatia.gazette.ocr.main",
            ext=None,
            no_recursive=True,
        )

        _add_root(arguments, database)

        assert captured[0].recursive is False  # type: ignore[attr-defined]

    def test_relative_path_rejected(self) -> None:
        database = _mock_database()
        arguments = argparse.Namespace(
            path="relative/path",
            rel_path="gazette",
            country="HR",
            group="ocr",
            worker_path="data_collector.croatia.gazette.ocr.main",
            ext=None,
            no_recursive=False,
        )

        with pytest.raises(SystemExit):
            _add_root(arguments, database)


# ---------------------------------------------------------------------------
# TestListRoots
# ---------------------------------------------------------------------------


class TestListRoots:
    """Test list-roots CLI command."""

    def test_lists_roots(self, capsys: pytest.CaptureFixture[str]) -> None:
        database = _mock_database()

        mock_row = MagicMock()
        mock_row.id = 1
        mock_row.active = True
        mock_row.country = "HR"
        mock_row.watch_group = "ocr"
        mock_row.root_path = "/ingest/hr/gazette"
        mock_row.worker_path = "data_collector.croatia.gazette.ocr.main"
        mock_row.extensions = '[".pdf"]'

        database.query.return_value.scalars.return_value.all.return_value = [mock_row]

        arguments = argparse.Namespace(country=None, group=None)
        _list_roots(arguments, database)

        captured = capsys.readouterr()
        assert "HR" in captured.out
        assert "/ingest/hr/gazette" in captured.out

    def test_empty_result(self, capsys: pytest.CaptureFixture[str]) -> None:
        database = _mock_database()
        database.query.return_value.scalars.return_value.all.return_value = []

        arguments = argparse.Namespace(country=None, group=None)
        _list_roots(arguments, database)

        captured = capsys.readouterr()
        assert "No watch roots found" in captured.out


# ---------------------------------------------------------------------------
# TestRemoveRoot
# ---------------------------------------------------------------------------


class TestRemoveRoot:
    """Test remove-root CLI command."""

    def test_sets_archive_timestamp(self) -> None:
        database = _mock_database()
        session = database.create_session.return_value.__enter__.return_value

        mock_row = MagicMock()
        mock_row.root_path = "/ingest/hr/gazette"
        database.query.return_value.scalar_one_or_none.return_value = mock_row

        arguments = argparse.Namespace(root_id=1)
        _remove_root(arguments, database)

        assert isinstance(mock_row.archive, datetime)
        session.commit.assert_called_once()

    def test_not_found_exits(self) -> None:
        database = _mock_database()
        database.query.return_value.scalar_one_or_none.return_value = None

        arguments = argparse.Namespace(root_id=999)

        with pytest.raises(SystemExit):
            _remove_root(arguments, database)


# ---------------------------------------------------------------------------
# TestActivateDeactivate
# ---------------------------------------------------------------------------


class TestActivateDeactivate:
    """Test activate-root and deactivate-root CLI commands."""

    def test_activate_sets_active_true(self) -> None:
        database = _mock_database()
        session = database.create_session.return_value.__enter__.return_value
        mock_row = MagicMock()
        mock_row.root_path = "/ingest/hr/gazette"
        database.query.return_value.scalar_one_or_none.return_value = mock_row

        arguments = argparse.Namespace(root_id=1)
        _activate_root(arguments, database)

        assert mock_row.active is True
        session.commit.assert_called_once()

    def test_deactivate_sets_active_false(self) -> None:
        database = _mock_database()
        session = database.create_session.return_value.__enter__.return_value
        mock_row = MagicMock()
        mock_row.root_path = "/ingest/hr/gazette"
        database.query.return_value.scalar_one_or_none.return_value = mock_row

        arguments = argparse.Namespace(root_id=1)
        _deactivate_root(arguments, database)

        assert mock_row.active is False
        session.commit.assert_called_once()

    def test_activate_not_found_exits(self) -> None:
        database = _mock_database()
        database.query.return_value.scalar_one_or_none.return_value = None

        arguments = argparse.Namespace(root_id=999)

        with pytest.raises(SystemExit):
            _activate_root(arguments, database)
