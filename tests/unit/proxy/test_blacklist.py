"""Tests for BlacklistChecker lockout logic and retention cleanup."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from data_collector.proxy.blacklist import DEFAULT_LOCKOUT_DURATIONS, BlacklistChecker
from data_collector.tables.proxy import ProxyBlacklist


def _make_mock_database() -> MagicMock:
    """Create a mock Database with a mock create_session context manager."""
    database = MagicMock()
    session = MagicMock()
    database.create_session.return_value.__enter__ = MagicMock(return_value=session)
    database.create_session.return_value.__exit__ = MagicMock(return_value=False)
    return database


class TestIsLockedOut:
    """Tests for BlacklistChecker.is_locked_out."""

    def test_not_locked_when_no_entry(self) -> None:
        database = _make_mock_database()
        database.query.return_value.scalar_one_or_none.return_value = None

        checker = BlacklistChecker(database, "sub.example.com")
        assert checker.is_locked_out("1.2.3.4") is False

    def test_locked_when_banned(self) -> None:
        database = _make_mock_database()
        row = MagicMock()
        row.is_banned = True
        row.lockout_until = None
        database.query.return_value.scalar_one_or_none.return_value = row

        checker = BlacklistChecker(database, "sub.example.com")
        assert checker.is_locked_out("1.2.3.4") is True

    def test_locked_when_lockout_active(self) -> None:
        database = _make_mock_database()
        row = MagicMock()
        row.is_banned = False
        row.lockout_until = datetime.now(UTC) + timedelta(minutes=10)
        database.query.return_value.scalar_one_or_none.return_value = row

        checker = BlacklistChecker(database, "sub.example.com")
        assert checker.is_locked_out("1.2.3.4") is True

    def test_not_locked_when_lockout_expired(self) -> None:
        database = _make_mock_database()
        row = MagicMock()
        row.is_banned = False
        row.lockout_until = datetime.now(UTC) - timedelta(minutes=1)
        database.query.return_value.scalar_one_or_none.return_value = row

        checker = BlacklistChecker(database, "sub.example.com")
        assert checker.is_locked_out("1.2.3.4") is False


class TestRecordFailure:
    """Tests for BlacklistChecker.record_failure."""

    def test_first_failure_creates_entry(self) -> None:
        database = _make_mock_database()
        session = database.create_session.return_value.__enter__.return_value
        database.query.return_value.scalar_one_or_none.return_value = None

        checker = BlacklistChecker(database, "sub.example.com")
        checker.record_failure("1.2.3.4")

        database.add.assert_called_once()
        added_entry = database.add.call_args[0][0]
        assert isinstance(added_entry, ProxyBlacklist)
        assert added_entry.ip_address == "1.2.3.4"  # type: ignore[comparison-overlap]
        assert added_entry.target_domain == "sub.example.com"  # type: ignore[comparison-overlap]
        assert added_entry.failure_count == 1  # type: ignore[comparison-overlap]
        assert added_entry.lockout_level == 0  # type: ignore[comparison-overlap]
        assert added_entry.is_banned is False
        session.commit.assert_called_once()

    def test_subsequent_failure_escalates_lockout(self) -> None:
        database = _make_mock_database()
        session = database.create_session.return_value.__enter__.return_value
        row = MagicMock()
        row.failure_count = 1
        row.lockout_level = 0
        database.query.return_value.scalar_one_or_none.return_value = row

        checker = BlacklistChecker(database, "sub.example.com")
        checker.record_failure("1.2.3.4")

        assert row.failure_count == 2
        assert row.lockout_level == 1
        assert row.is_banned is not True
        session.commit.assert_called_once()

    def test_max_level_triggers_permanent_ban(self) -> None:
        database = _make_mock_database()
        session = database.create_session.return_value.__enter__.return_value
        row = MagicMock()
        row.failure_count = 5
        row.lockout_level = len(DEFAULT_LOCKOUT_DURATIONS) - 1
        database.query.return_value.scalar_one_or_none.return_value = row

        checker = BlacklistChecker(database, "sub.example.com")
        checker.record_failure("1.2.3.4")

        assert row.is_banned is True
        assert row.lockout_until is None
        session.commit.assert_called_once()


class TestCleanupExpired:
    """Tests for BlacklistChecker.cleanup_expired."""

    def test_returns_deleted_count(self) -> None:
        database = _make_mock_database()
        session = database.create_session.return_value.__enter__.return_value
        database.run.return_value.rowcount = 5

        checker = BlacklistChecker(database, "sub.example.com", retention_days=30)
        result = checker.cleanup_expired()

        assert result == 5
        session.commit.assert_called_once()

    def test_returns_zero_when_nothing_to_clean(self) -> None:
        database = _make_mock_database()
        database.run.return_value.rowcount = 0

        checker = BlacklistChecker(database, "sub.example.com")
        result = checker.cleanup_expired()

        assert result == 0


class TestDefaultLockoutDurations:
    """Tests for default lockout progression constants."""

    def test_has_five_levels(self) -> None:
        assert len(DEFAULT_LOCKOUT_DURATIONS) == 5

    def test_durations_are_increasing(self) -> None:
        for index in range(1, len(DEFAULT_LOCKOUT_DURATIONS)):
            assert DEFAULT_LOCKOUT_DURATIONS[index] > DEFAULT_LOCKOUT_DURATIONS[index - 1]

    def test_first_duration_is_five_minutes(self) -> None:
        assert DEFAULT_LOCKOUT_DURATIONS[0] == timedelta(minutes=5)

    def test_last_duration_is_24_hours(self) -> None:
        assert DEFAULT_LOCKOUT_DURATIONS[-1] == timedelta(hours=24)
