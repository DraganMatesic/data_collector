"""Unit tests for scaffold generator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data_collector.enums import AppType, FatalFlag, RunStatus
from data_collector.scaffold.generator import (
    disable_app,
    enable_app,
    remove_app,
    scaffold_app,
    to_class_name,
    unmanage_app,
)

_MODULE = "data_collector.scaffold.generator"


class TestToClassName:
    """Test snake_case to PascalCase conversion."""

    def test_single_word(self) -> None:
        assert to_class_name("company") == "Company"

    def test_two_words(self) -> None:
        assert to_class_name("company_data") == "CompanyData"

    def test_three_words(self) -> None:
        assert to_class_name("court_case_detail") == "CourtCaseDetail"

    def test_already_single_word_capitalized(self) -> None:
        assert to_class_name("Data") == "Data"


class TestScaffoldApp:
    """Test scaffold_app() file generation."""

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_creates_files_single(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="single", _package_root=tmp_path)

        app_dir = tmp_path / "test" / "demo" / "hello_world"
        assert (app_dir / "__init__.py").exists()
        assert (app_dir / "main.py").exists()
        assert (app_dir / "parser.py").exists()
        assert (app_dir / "tables.py").exists()

        main_content = (app_dir / "main.py").read_text()
        assert "class HelloWorld(BaseScraper)" in main_content
        assert "ThreadPoolExecutor" not in main_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_creates_files_threaded(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="threaded", _package_root=tmp_path)

        app_dir = tmp_path / "test" / "demo" / "hello_world"
        main_content = (app_dir / "main.py").read_text()
        assert "class HelloWorld(ThreadedScraper)" in main_content
        assert "process_batch" in main_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_creates_group_and_parent_init(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="cro", parent="financials", name="company_data", _package_root=tmp_path)

        assert (tmp_path / "cro" / "__init__.py").exists()
        assert (tmp_path / "cro" / "financials" / "__init__.py").exists()

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_aborts_if_directory_exists(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        (tmp_path / "test" / "demo" / "hello_world").mkdir(parents=True)

        with pytest.raises(SystemExit) as exc_info:
            scaffold_app(group="test", parent="demo", name="hello_world", _package_root=tmp_path)
        assert exc_info.value.code == 1

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_parser_template_content(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="my_app", _package_root=tmp_path)

        parser_content = (tmp_path / "test" / "demo" / "my_app" / "parser.py").read_text()
        assert "class Parser:" in parser_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_tables_template_content(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="my_app", _package_root=tmp_path)

        tables_content = (tmp_path / "test" / "demo" / "my_app" / "tables.py").read_text()
        assert "class MyAppRecord(Base)" in tables_content
        assert "auto_increment_column()" in tables_content
        assert "sha = Column(" in tables_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_registers_app_in_db(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", _package_root=tmp_path)

        mock_register.assert_called_once()
        assert mock_register.call_args[0][0] == "test"
        assert mock_register.call_args[0][1] == "demo"
        assert mock_register.call_args[0][2] == "hello_world"

    @patch(f"{_MODULE}._register_app_in_db", return_value=False)
    def test_creates_files_even_when_db_unavailable(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", _package_root=tmp_path)

        app_dir = tmp_path / "test" / "demo" / "hello_world"
        assert (app_dir / "main.py").exists()
        captured = capsys.readouterr()
        assert "NOT registered" in captured.out

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_init_template_has_module_docstring(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="my_app", _package_root=tmp_path)

        init_content = (tmp_path / "test" / "demo" / "my_app" / "__init__.py").read_text()
        assert "test.demo.my_app" in init_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_output_shows_next_steps(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        scaffold_app(group="test", parent="demo", name="my_app", _package_root=tmp_path)

        captured = capsys.readouterr()
        assert "Next steps:" in captured.out
        assert "python -m data_collector.test.demo.my_app.main" in captured.out

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_creates_files_async(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="async", _package_root=tmp_path)

        app_dir = tmp_path / "test" / "demo" / "hello_world"
        assert (app_dir / "__init__.py").exists()
        assert (app_dir / "main.py").exists()
        assert (app_dir / "parser.py").exists()
        assert (app_dir / "tables.py").exists()

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_async_template_content(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="async", _package_root=tmp_path)

        main_content = (tmp_path / "test" / "demo" / "hello_world" / "main.py").read_text()
        assert "class HelloWorld(AsyncScraper)" in main_content
        assert "process_batch_async" in main_content
        assert "asyncio.run(scraper.collect())" in main_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_threaded_template_uses_threaded_scraper(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="threaded", _package_root=tmp_path)

        main_content = (tmp_path / "test" / "demo" / "hello_world" / "main.py").read_text()
        assert "class HelloWorld(ThreadedScraper)" in main_content
        assert "process_batch" in main_content
        assert "create_worker_request" in main_content


    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_creates_files_dramatiq(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="dramatiq", _package_root=tmp_path)

        app_dir = tmp_path / "test" / "demo" / "hello_world"
        assert (app_dir / "__init__.py").exists()
        assert (app_dir / "main.py").exists()
        assert (app_dir / "topics.py").exists()

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_dramatiq_no_parser_or_tables(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="dramatiq", _package_root=tmp_path)

        app_dir = tmp_path / "test" / "demo" / "hello_world"
        assert not (app_dir / "parser.py").exists()
        assert not (app_dir / "tables.py").exists()

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_dramatiq_template_content(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="dramatiq", _package_root=tmp_path)

        main_content = (tmp_path / "test" / "demo" / "hello_world" / "main.py").read_text()
        assert "@dramatiq.actor" in main_content
        assert "FunWatchMixin" in main_content
        assert "class HelloWorldProcessor" in main_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_dramatiq_topics_content(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="dramatiq", _package_root=tmp_path)

        topics_content = (tmp_path / "test" / "demo" / "hello_world" / "topics.py").read_text()
        assert "MAIN_EXCHANGE_QUEUE" in topics_content
        assert "TopicExchangeQueue" in topics_content

    @patch(f"{_MODULE}._register_app_in_db", return_value=True)
    def test_dramatiq_registers_as_dramatiq_type(
        self,
        mock_register: MagicMock,
        tmp_path: Path,
    ) -> None:
        scaffold_app(group="test", parent="demo", name="hello_world", app_type="dramatiq", _package_root=tmp_path)

        mock_register.assert_called_once()
        call_kwargs = mock_register.call_args
        assert call_kwargs[1]["resolved_app_type"] == AppType.DRAMATIQ


def _make_mock_database(mock_app: MagicMock | None) -> MagicMock:
    """Build a mock Database whose session context manager and query chain return ``mock_app``."""
    mock_database = MagicMock()
    mock_session = MagicMock()
    mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
    mock_database.query.return_value.scalar_one_or_none.return_value = mock_app
    return mock_database


class TestEnableApp:
    """Tests for enable_app lifecycle function."""

    @patch(f"{_MODULE}.MainDatabaseSettings")
    @patch(f"{_MODULE}.Database")
    def test_enable_app_sets_disable_false(
        self,
        mock_database_class: MagicMock,
        mock_settings_class: MagicMock,
    ) -> None:
        mock_app = MagicMock()
        mock_app.app_type = AppType.MANAGED
        mock_database = _make_mock_database(mock_app)
        mock_database_class.return_value = mock_database

        enable_app("test", "demo", "hello")

        assert mock_app.disable is False
        assert mock_app.fatal_flag == FatalFlag.NONE
        assert mock_app.fatal_msg is None
        assert mock_app.fatal_time is None
        mock_database.create_session.return_value.__enter__.return_value.commit.assert_called_once()

    @patch(f"{_MODULE}.MainDatabaseSettings")
    @patch(f"{_MODULE}.Database")
    def test_enable_app_refuses_unmanaged(
        self,
        mock_database_class: MagicMock,
        mock_settings_class: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        mock_app = MagicMock()
        mock_app.app_type = AppType.STANDALONE
        mock_database = _make_mock_database(mock_app)
        mock_database_class.return_value = mock_database

        enable_app("test", "demo", "hello")

        captured = capsys.readouterr()
        assert "is not a managed app" in captured.err
        mock_database.create_session.return_value.__enter__.return_value.commit.assert_not_called()


class TestDisableApp:
    """Tests for disable_app lifecycle function."""

    @patch(f"{_MODULE}.MainDatabaseSettings")
    @patch(f"{_MODULE}.Database")
    def test_disable_app_sets_disable_true(
        self,
        mock_database_class: MagicMock,
        mock_settings_class: MagicMock,
    ) -> None:
        mock_app = MagicMock()
        mock_database = _make_mock_database(mock_app)
        mock_database_class.return_value = mock_database

        disable_app("test", "demo", "hello")

        assert mock_app.disable is True
        mock_database.create_session.return_value.__enter__.return_value.commit.assert_called_once()


class TestUnmanageApp:
    """Tests for unmanage_app lifecycle function."""

    @patch(f"{_MODULE}.MainDatabaseSettings")
    @patch(f"{_MODULE}.Database")
    def test_unmanage_app_sets_managed_false(
        self,
        mock_database_class: MagicMock,
        mock_settings_class: MagicMock,
    ) -> None:
        mock_app = MagicMock()
        mock_database = _make_mock_database(mock_app)
        mock_database_class.return_value = mock_database

        unmanage_app("test", "demo", "hello")

        assert mock_app.app_type == AppType.STANDALONE
        assert mock_app.disable is True
        mock_database.create_session.return_value.__enter__.return_value.commit.assert_called_once()


class TestRemoveApp:
    """Tests for remove_app lifecycle function."""

    @patch(f"{_MODULE}.MainDatabaseSettings")
    @patch(f"{_MODULE}.Database")
    def test_remove_app_sets_removal_date(
        self,
        mock_database_class: MagicMock,
        mock_settings_class: MagicMock,
    ) -> None:
        mock_app = MagicMock()
        mock_app.run_status = RunStatus.NOT_RUNNING
        mock_database = _make_mock_database(mock_app)
        mock_database_class.return_value = mock_database

        remove_app("test", "demo", "hello", grace_days=7)

        assert mock_app.removal_date is not None
        assert mock_app.app_type == AppType.STANDALONE
        assert mock_app.disable is True
        mock_database.create_session.return_value.__enter__.return_value.commit.assert_called_once()

    @patch(f"{_MODULE}.MainDatabaseSettings")
    @patch(f"{_MODULE}.Database")
    def test_remove_app_refuses_running(
        self,
        mock_database_class: MagicMock,
        mock_settings_class: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        mock_app = MagicMock()
        mock_app.run_status = RunStatus.RUNNING
        mock_database = _make_mock_database(mock_app)
        mock_database_class.return_value = mock_database

        remove_app("test", "demo", "hello")

        captured = capsys.readouterr()
        assert "currently running" in captured.err
        mock_database.create_session.return_value.__enter__.return_value.commit.assert_not_called()
