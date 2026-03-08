"""Tests for ProxyManager acquisition, reservation, and lifecycle."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from data_collector.proxy.models import Proxy, ProxyAcquisitionTimeout, ProxyData
from data_collector.proxy.provider import BrightDataProvider
from data_collector.proxy.proxy_manager import ProxyManager
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


class TestProxyManagerInit:
    """Tests for ProxyManager constructor."""

    def test_extracts_reservation_domain(self) -> None:
        database = _make_mock_database()
        provider = _make_provider()
        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="sub.gov.de",
            app_id="a" * 64,
        )
        assert proxy_manager.reservation_domain == "gov.de"
        assert proxy_manager.target_domain == "sub.gov.de"

    def test_default_parameters(self) -> None:
        database = _make_mock_database()
        provider = _make_provider()
        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
        )
        assert proxy_manager.ttl_seconds == 1800
        assert proxy_manager.acquire_timeout == 120
        assert proxy_manager.recheck_interval == 5

    def test_custom_judges(self) -> None:
        database = _make_mock_database()
        provider = _make_provider()
        custom_judges = ["https://custom-judge.com/ip"]
        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
            judges=custom_judges,
        )
        assert proxy_manager.judges is custom_judges


class TestAcquire:
    """Tests for ProxyManager.acquire."""

    @patch("data_collector.proxy.proxy_manager.verify_ip")
    def test_successful_acquisition(self, mock_verify_ip: MagicMock) -> None:
        mock_verify_ip.return_value = "1.2.3.4"
        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="sub.example.com",
            app_id="a" * 64,
        )

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
        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="sub.example.com",
            app_id="a" * 64,
        )

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

        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
            acquire_timeout=120,
        )

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

        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
            acquire_timeout=120,
        )

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

        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
        )

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
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="sub.example.com",
            app_id="a" * 64,
        )

        proxy_manager.release("1.2.3.4")

        session = database.create_session.return_value.__enter__.return_value
        session.execute.assert_called_once()
        session.commit.assert_called_once()


class TestReportFailure:
    """Tests for ProxyManager.report_failure."""

    def test_delegates_to_blacklist_checker(self) -> None:
        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="sub.example.com",
            app_id="a" * 64,
        )

        with patch.object(proxy_manager.blacklist_checker, "record_failure") as mock_record:
            proxy_manager.report_failure("1.2.3.4")

        mock_record.assert_called_once_with("1.2.3.4")


class TestTryReserve:
    """Tests for ProxyManager._try_reserve."""

    def test_successful_reservation(self) -> None:
        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
        )

        result = proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        assert result is True
        session = database.create_session.return_value.__enter__.return_value
        session.add.assert_called_once()
        session.commit.assert_called_once()

    def test_integrity_error_returns_false(self) -> None:
        database = _make_mock_database()
        session = database.create_session.return_value.__enter__.return_value
        session.commit.side_effect = IntegrityError(
            "duplicate", {}, Exception("unique constraint violated")
        )

        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="example.com",
            app_id="a" * 64,
        )

        result = proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        assert result is False
        session.rollback.assert_called_once()

    def test_reservation_uses_root_domain(self) -> None:
        database = _make_mock_database()
        provider = _make_provider()

        proxy_manager = ProxyManager(
            provider=provider,
            database=database,
            target_domain="sub.example.com",
            app_id="a" * 64,
        )

        proxy_manager._try_reserve("1.2.3.4")  # pyright: ignore[reportPrivateUsage]

        session = database.create_session.return_value.__enter__.return_value
        added_reservation = session.add.call_args[0][0]
        assert isinstance(added_reservation, ProxyReservation)
        assert added_reservation.target_domain == "example.com"  # type: ignore[comparison-overlap]
