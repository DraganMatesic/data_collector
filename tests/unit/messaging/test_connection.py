"""Tests for RabbitMQConnection."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pika.exceptions
import pytest

from data_collector.messaging.connection import RabbitMQConnection
from data_collector.settings.rabbitmq import RabbitMQSettings


def _make_settings(**overrides: object) -> RabbitMQSettings:
    defaults: dict[str, object] = {
        "reconnect_max_attempts": 3,
        "reconnect_base_delay": 0,
        "reconnect_max_delay": 0,
    }
    defaults.update(overrides)
    return RabbitMQSettings(**defaults)  # type: ignore[arg-type]


class TestConnect:
    """Tests for connection establishment."""

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_connect_creates_connection_and_channel(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()

        mock_blocking.assert_called_once()
        mock_connection.channel.assert_called_once()

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_connect_sets_prefetch(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings(prefetch=5))
        connection.connect()

        mock_channel.basic_qos.assert_called_once_with(prefetch_count=5)

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_connect_uses_credentials(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_connection.channel.return_value = MagicMock()
        mock_blocking.return_value = mock_connection

        settings = _make_settings(
            host="myhost",
            port=5673,
            username="admin",
            password="secret",
        )
        connection = RabbitMQConnection(settings)
        connection.connect()

        call_args = mock_blocking.call_args
        parameters = call_args[0][0]
        assert parameters.host == "myhost"
        assert parameters.port == 5673


class TestClose:
    """Tests for connection closing."""

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_close_closes_channel_and_connection(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_open = True
        mock_channel.is_open = True
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()
        connection.close()

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_close_swallows_exceptions(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_channel.close.side_effect = pika.exceptions.AMQPError("already closed")
        mock_connection.is_open = True
        mock_connection.close.side_effect = pika.exceptions.AMQPError("already closed")
        mock_connection.channel.return_value = mock_channel
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()
        connection.close()  # Should not raise

    def test_close_when_not_connected(self) -> None:
        connection = RabbitMQConnection(_make_settings())
        connection.close()  # Should not raise


class TestIsHealthy:
    """Tests for health check."""

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_healthy_when_open(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.is_open = True
        mock_channel.is_open = True
        mock_connection.channel.return_value = mock_channel
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()
        assert connection.is_healthy() is True

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_unhealthy_when_channel_closed(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.is_open = True
        mock_channel.is_open = False
        mock_connection.channel.return_value = mock_channel
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()
        assert connection.is_healthy() is False

    def test_unhealthy_when_no_connection(self) -> None:
        connection = RabbitMQConnection(_make_settings())
        assert connection.is_healthy() is False


class TestEnsureConnected:
    """Tests for ensure_connected and reconnection."""

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_noop_when_open(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.is_open = True
        mock_channel.is_open = True
        mock_connection.channel.return_value = mock_channel
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()
        channel = connection.ensure_connected()

        assert channel is mock_channel
        assert mock_blocking.call_count == 1

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_reconnects_when_closed(self, mock_blocking: MagicMock) -> None:
        mock_connection_1 = MagicMock()
        mock_channel_1 = MagicMock()
        mock_connection_1.is_open = True
        mock_channel_1.is_open = True
        mock_connection_1.channel.return_value = mock_channel_1

        mock_connection_2 = MagicMock()
        mock_channel_2 = MagicMock()
        mock_connection_2.is_open = True
        mock_channel_2.is_open = True
        mock_connection_2.channel.return_value = mock_channel_2

        mock_blocking.side_effect = [mock_connection_1, mock_connection_2]

        connection = RabbitMQConnection(_make_settings())
        connection.connect()

        # Simulate connection drop
        mock_connection_1.is_open = False

        channel = connection.ensure_connected()
        assert channel is mock_channel_2
        assert mock_blocking.call_count == 2

    @patch("data_collector.messaging.connection.time.sleep")
    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_reconnect_retries_with_backoff(
        self, mock_blocking: MagicMock, mock_sleep: MagicMock
    ) -> None:
        mock_blocking.side_effect = [
            pika.exceptions.AMQPConnectionError("fail 1"),
            pika.exceptions.AMQPConnectionError("fail 2"),
            MagicMock(channel=MagicMock(return_value=MagicMock(is_open=True)), is_open=True),
        ]

        settings = _make_settings(
            reconnect_base_delay=1,
            reconnect_max_delay=10,
        )
        connection = RabbitMQConnection(settings)
        connection.ensure_connected()

        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0][0][0] == 1  # base_delay * 2^0
        assert mock_sleep.call_args_list[1][0][0] == 2  # base_delay * 2^1

    @patch("data_collector.messaging.connection.time.sleep")
    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_raises_after_max_attempts(
        self, mock_blocking: MagicMock, mock_sleep: MagicMock
    ) -> None:
        mock_blocking.side_effect = pika.exceptions.AMQPConnectionError("unreachable")

        connection = RabbitMQConnection(_make_settings(reconnect_max_attempts=3))
        with pytest.raises(ConnectionError, match="Failed to connect.*3 attempts"):
            connection.ensure_connected()


class TestProperties:
    """Tests for channel and connection properties."""

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_channel_property(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.is_open = True
        mock_channel.is_open = True
        mock_connection.channel.return_value = mock_channel
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()
        assert connection.channel is mock_channel

    def test_connection_property_raises_when_not_connected(self) -> None:
        connection = RabbitMQConnection(_make_settings())
        with pytest.raises(ConnectionError, match="No active"):
            _ = connection.connection

    @patch("data_collector.messaging.connection.pika.BlockingConnection")
    def test_connection_property_returns_connection(self, mock_blocking: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_connection.is_open = True
        mock_connection.channel.return_value = MagicMock()
        mock_blocking.return_value = mock_connection

        connection = RabbitMQConnection(_make_settings())
        connection.connect()
        assert connection.connection is mock_connection

    def test_settings_property(self) -> None:
        settings = _make_settings()
        connection = RabbitMQConnection(settings)
        assert connection.settings is settings
