"""Tests for ProxyManager acquisition, reservation, and lifecycle."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from data_collector.proxy.models import Proxy, ProxyAcquisitionTimeout, ProxyData
from data_collector.proxy.provider import BrightDataProvider
from data_collector.proxy.proxy_manager import ProxyManager, cleanup_all_reservations
from data_collector.tables.proxy import ProxyReservation


def _make_mock_database() -> MagicMock:
    """Create a mock Database with a mock create_session context manager."""
    database = MagicMock()
    session = MagicMock()
    database.create_session.return_value.__enter__ = MagicMock(return_value=session)
    database.create_session.return_value.__exit__ = MagicMock(return_value=False)
    return database


def _make_provider() -> BrightDataProvider:
    """Create a BrightDataProvider with test credentials."""
    proxy_data = ProxyData(
        host="proxy.test.com",
        port=22225,
        username="test-user",
        password="test-pass",
        country="hr",
    )
    return BrightDataProvider(proxy_data)


def _make_proxy_manager(**kwargs: object) -> ProxyManager:
    """Create a ProxyManager with test defaults and stop the daemon thread.

    The daemon thread is stopped immediately to prevent background DB calls
    during tests. Tests that need the thread running should start it explicitly.
    """
    defaults: dict[str, object] = {
        "provider": _make_provider(),
        "database": _make_mock_database(),
        "target_domain": "example.com",
        "app_id": "a" * 64,
    }
    defaults.update(kwargs)
    proxy_manager = ProxyManager(**defaults)  # type: ignore[arg-type]
    proxy_manager.stop()
    return proxy_manager


class TestProxyManagerInit:
    """Tests for ProxyManager constructor."""

    def test_extracts_reservation_domain(self) -> None:
        proxy_manager = _make_proxy_manager(target_domain="sub.gov.hr")
        assert proxy_manager.reservation_domain == "gov.hr"
        assert proxy_manager.target_domain == "sub.gov.hr"

    def test_default_parameters(self) -> None:
        proxy_manager = _make_proxy_manager()
        assert proxy_manager.ttl_seconds == 1800
        assert proxy_manager.cooldown_seconds == 300
        assert proxy_manager.cleanup_interval == 300
        assert proxy_manager.acquire_timeout == 120
        assert proxy_manager.recheck_interval == 5

    def test_cleanup_interval_defaults_to_cooldown(self) -> None:
        proxy_manager = _make_proxy_manager(cooldown_seconds=10)
        assert proxy_manager.cleanup_interval == 10

    def test_cleanup_interval_explicit_override(self) -> None:
        proxy_manager = _make_proxy_manager(cooldown_seconds=10, cleanup_interval=30)
        assert proxy_manager.cleanup_interval == 30

    def test_custom_judges(self) -> None:
        custom_judges = ["https://custom-judge.com/ip"]
        proxy_manager = _make_proxy_manager(judges=custom_judges)
        assert proxy_manager.judges is custom_judges

    def test_cleanup_thread_starts_on_init(self) -> None:
        database = _make_mock_database()
        proxy_manager = ProxyManager(
            provider=_make_provider(),
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
        )
        assert proxy_manager._cleanup_thread.is_alive()  # pyright: ignore[reportPrivateUsage]
        assert proxy_manager._cleanup_thread.daemon  # pyright: ignore[reportPrivateUsage]
        proxy_manager.stop()

    def test_cleanup_thread_stops_on_stop(self) -> None:
        database = _make_mock_database()
        proxy_manager = ProxyManager(
            provider=_make_provider(),
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
            cleanup_interval=1,
        )
        proxy_manager.stop()
        proxy_manager._cleanup_thread.join(timeout=3)  # pyright: ignore[reportPrivateUsage]
        assert not proxy_manager._cleanup_thread.is_alive()  # pyright: ignore[reportPrivateUsage]


class TestAcquire:
    """Tests for ProxyManager.acquire."""

    @patch("data_collector.proxy.proxy_manager.verify_ip")
    def test_successful_acquisition(self, mock_verify_ip: MagicMock) -> None:
        mock_verify_ip.return_value = "1.2.3.4"
        provider = _make_provider()
        proxy_manager = _make_proxy_manager(provider=provider, target_domain="sub.example.com")

        with (
            patch.object(proxy_manager, "_try_reserve", return_value=True),
            patch.object(proxy_manager.blacklist_checker, "is_locked_out", return_value=False),
            patch.object(provider, "is_healthy", return_value=True),
        ):
            logger = MagicMock()
            result = proxy_manager.acquire(logger)

        assert isinstance(result, Proxy)
        assert result.ip_address == "1.2.3.4"
        assert result.target_domain == "sub.example.com"

    @patch("data_collector.proxy.proxy_manager.verify_ip")
    def test_skips_blacklisted_ip(self, mock_verify_ip: MagicMock) -> None:
        mock_verify_ip.side_effect = ["1.2.3.4", "5.6.7.8"]
        provider = _make_provider()
        proxy_manager = _make_proxy_manager(provider=provider, target_domain="sub.example.com")

        locked_out_calls = [True, False]

        with (
            patch.object(proxy_manager, "_try_reserve", return_value=True),
            patch.object(proxy_manager.blacklist_checker, "is_locked_out", side_effect=locked_out_calls),
            patch.object(provider, "is_healthy", return_value=True),
        ):
            logger = MagicMock()
            result = proxy_manager.acquire(logger)

        assert result.ip_address == "5.6.7.8"

    @patch("data_collector.proxy.proxy_manager.verify_ip")
    @patch("data_collector.proxy.proxy_manager.time")
    def test_retries_on_reservation_failure(self, mock_time: MagicMock, mock_verify_ip: MagicMock) -> None:
        mock_verify_ip.return_value = "1.2.3.4"
        mock_time.monotonic.side_effect = [0.0, 1.0, 2.0, 3.0, 4.0]
        mock_time.sleep = MagicMock()

        provider = _make_provider()
        proxy_manager = _make_proxy_manager(provider=provider, acquire_timeout=120)

        reserve_calls = [False, True]

        with (
            patch.object(proxy_manager, "_try_reserve", side_effect=reserve_calls),
            patch.object(proxy_manager.blacklist_checker, "is_locked_out", return_value=False),
            patch.object(provider, "is_healthy", return_value=True),
        ):
            logger = MagicMock()
            result = proxy_manager.acquire(logger)

        assert result.ip_address == "1.2.3.4"

    @patch("data_collector.proxy.proxy_manager.verify_ip")
    @patch("data_collector.proxy.proxy_manager.time")
    def test_timeout_raises_exception(self, mock_time: MagicMock, mock_verify_ip: MagicMock) -> None:
        mock_verify_ip.return_value = None
        mock_time.monotonic.side_effect = [0.0, 200.0]

        provider = _make_provider()
        proxy_manager = _make_proxy_manager(provider=provider, acquire_timeout=120)

        with patch.object(provider, "is_healthy", return_value=True):
            logger = MagicMock()
            with pytest.raises(ProxyAcquisitionTimeout, match="example.com"):
                proxy_manager.acquire(logger)

    @patch("data_collector.proxy.proxy_manager.verify_ip")
    @patch("data_collector.proxy.proxy_manager.time")
    def test_health_check_failure_retries(self, mock_time: MagicMock, mock_verify_ip: MagicMock) -> None:
        mock_verify_ip.return_value = "1.2.3.4"
        mock_time.monotonic.side_effect = [0.0, 1.0, 2.0, 3.0, 4.0]
        mock_time.sleep = MagicMock()

        provider = _make_provider()
        proxy_manager = _make_proxy_manager(provider=provider)

        health_calls = [False, True]

        with (
            patch.object(proxy_manager, "_try_reserve", return_value=True),
            patch.object(proxy_manager.blacklist_checker, "is_locked_out", return_value=False),
            patch.object(provider, "is_healthy", side_effect=health_calls),
        ):
            logger = MagicMock()
            result = proxy_manager.acquire(logger)

        assert result.ip_address == "1.2.3.4"


class TestRelease:
    """Tests for ProxyManager.release."""

    def test_release_updates_reservation(self) -> None:
        database = _make_mock_database()
        proxy_manager = _make_proxy_manager(database=database, target_domain="sub.example.com")

        proxy_manager.release("1.2.3.4")

        database.run.assert_called_once()
        session = database.create_session.return_value.__enter__.return_value
        session.commit.assert_called_once()


class TestReportFailure:
    """Tests for ProxyManager.report_failure."""

    def test_delegates_to_blacklist_checker(self) -> None:
        proxy_manager = _make_proxy_manager(target_domain="sub.example.com")

        with patch.object(proxy_manager.blacklist_checker, "record_failure") as mock_record:
            proxy_manager.report_failure("1.2.3.4")

        mock_record.assert_called_once_with("1.2.3.4")


class TestTryReserve:
    """Tests for ProxyManager._try_reserve."""

    def test_successful_reservation(self) -> None:
        database = _make_mock_database()
        database.query.return_value.first.return_value = None
        proxy_manager = _make_proxy_manager(database=database)

        result = proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        assert result is True
        database.add.assert_called_once()
        session = database.create_session.return_value.__enter__.return_value
        session.commit.assert_called_once()

    def test_integrity_error_returns_false(self) -> None:
        database = _make_mock_database()
        database.query.return_value.first.return_value = None
        session = database.create_session.return_value.__enter__.return_value
        session.commit.side_effect = IntegrityError(
            "duplicate", {}, Exception("unique constraint violated")
        )

        proxy_manager = _make_proxy_manager(database=database)

        result = proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        assert result is False
        session.rollback.assert_called_once()

    def test_reservation_uses_root_domain(self) -> None:
        database = _make_mock_database()
        database.query.return_value.first.return_value = None
        proxy_manager = _make_proxy_manager(database=database, target_domain="sub.example.com")

        proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        added_reservation = database.add.call_args[0][0]
        assert isinstance(added_reservation, ProxyReservation)
        assert added_reservation.target_domain == "example.com"  # type: ignore[comparison-overlap]

    def test_skips_ip_in_cooldown(self) -> None:
        database = _make_mock_database()
        database.query.return_value.first.return_value = MagicMock()  # Cooldown row exists
        proxy_manager = _make_proxy_manager(database=database, cooldown_seconds=300)

        result = proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        assert result is False
        database.add.assert_not_called()

    def test_allows_ip_past_cooldown(self) -> None:
        database = _make_mock_database()
        database.query.return_value.first.return_value = None  # No cooldown row
        proxy_manager = _make_proxy_manager(database=database, cooldown_seconds=300)

        result = proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        assert result is True
        database.add.assert_called_once()


class TestCleanupReservations:
    """Tests for ProxyManager.cleanup_reservations."""

    def test_deletes_own_past_cooldown(self) -> None:
        database = _make_mock_database()
        database.run.return_value.rowcount = 3
        proxy_manager = _make_proxy_manager(database=database, cooldown_seconds=300)

        result = proxy_manager.cleanup_reservations()

        assert result == 3
        database.run.assert_called_once()
        session = database.create_session.return_value.__enter__.return_value
        session.commit.assert_called_once()

    def test_returns_zero_when_no_rows(self) -> None:
        database = _make_mock_database()
        database.run.return_value.rowcount = 0
        proxy_manager = _make_proxy_manager(database=database)

        result = proxy_manager.cleanup_reservations()

        assert result == 0


class TestShutdown:
    """Tests for ProxyManager.shutdown."""

    def test_deletes_all_released_ignoring_cooldown(self) -> None:
        database = _make_mock_database()
        database.run.return_value.rowcount = 2
        proxy_manager = _make_proxy_manager(database=database, cooldown_seconds=300)

        proxy_manager.shutdown()

        database.run.assert_called_once()
        session = database.create_session.return_value.__enter__.return_value
        session.commit.assert_called_once()

    def test_stops_daemon_thread(self) -> None:
        database = _make_mock_database()
        proxy_manager = ProxyManager(
            provider=_make_provider(),
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
            cleanup_interval=1,
        )

        proxy_manager.shutdown()

        assert proxy_manager._stop_event.is_set()  # pyright: ignore[reportPrivateUsage]
        proxy_manager._cleanup_thread.join(timeout=3)  # pyright: ignore[reportPrivateUsage]
        assert not proxy_manager._cleanup_thread.is_alive()  # pyright: ignore[reportPrivateUsage]


class TestCleanupAllReservations:
    """Tests for cleanup_all_reservations janitor function."""

    def test_deletes_released_and_orphans(self) -> None:
        database = _make_mock_database()
        database.run.return_value.rowcount = 5

        result = cleanup_all_reservations(database, cooldown_seconds=300, ttl_seconds=1800)

        assert result == 10  # 5 released + 5 orphans (two separate calls)
        assert database.run.call_count == 2

    def test_returns_zero_when_no_rows(self) -> None:
        database = _make_mock_database()
        database.run.return_value.rowcount = 0

        result = cleanup_all_reservations(database)

        assert result == 0
