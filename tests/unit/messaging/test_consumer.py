"""Tests for CommandConsumer."""

from __future__ import annotations

import json
import threading
from unittest.mock import MagicMock, patch

import pika.exceptions
import pika.spec
import pytest

from data_collector.enums.commands import CmdName
from data_collector.messaging.consumer import CommandConsumer
from data_collector.messaging.models import CommandMessage
from data_collector.settings.rabbitmq import RabbitMQSettings


def _make_settings(**overrides: object) -> RabbitMQSettings:
    defaults: dict[str, object] = {
        "queue": "dc_test",
        "reconnect_base_delay": 0,
        "reconnect_max_delay": 0,
        "reconnect_max_attempts": 3,
    }
    defaults.update(overrides)
    return RabbitMQSettings(**defaults)  # type: ignore[arg-type]


def _make_body(**overrides: object) -> bytes:
    payload: dict[str, object] = {
        "app_id": "abc123",
        "command": 1,
        "issued_by": "admin@example.com",
        "timestamp": "2025-01-15T10:30:00+00:00",
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


def _make_deliver(routing_key: str = "dc_test", delivery_tag: int = 1) -> MagicMock:
    deliver = MagicMock(spec=pika.spec.Basic.Deliver)
    deliver.routing_key = routing_key
    deliver.delivery_tag = delivery_tag
    return deliver


class TestConsumerLifecycle:
    """Tests for start/stop and thread management."""

    def test_start_creates_daemon_thread(self) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.ensure_connected.return_value = mock_channel

        # Make start_consuming block until stop
        stop_event = threading.Event()
        mock_channel.start_consuming.side_effect = lambda: stop_event.wait(timeout=1.0)

        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)
        consumer.start()

        assert consumer.is_running is True
        assert consumer._consumer_thread is not None  # pyright: ignore[reportPrivateUsage]
        assert consumer._consumer_thread.daemon is True  # pyright: ignore[reportPrivateUsage]

        consumer._stop_event.set()  # pyright: ignore[reportPrivateUsage]
        stop_event.set()
        consumer._consumer_thread.join(timeout=2.0)  # pyright: ignore[reportPrivateUsage]

    def test_start_raises_if_already_running(self) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.ensure_connected.return_value = mock_channel

        stop_event = threading.Event()
        mock_channel.start_consuming.side_effect = lambda: stop_event.wait(timeout=1.0)

        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)
        consumer.start()

        with pytest.raises(RuntimeError, match="already running"):
            consumer.start()

        consumer._stop_event.set()  # pyright: ignore[reportPrivateUsage]
        stop_event.set()
        consumer._consumer_thread.join(timeout=2.0)  # pyright: ignore[reportPrivateUsage, reportOptionalMemberAccess]

    def test_stop_sets_event(self) -> None:
        mock_connection = MagicMock()
        mock_connection.connection = MagicMock()
        mock_connection.connection.is_open = False

        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)
        consumer._consumer_thread = MagicMock()  # pyright: ignore[reportPrivateUsage]
        consumer._consumer_thread.is_alive.return_value = False  # pyright: ignore[reportPrivateUsage]

        consumer.stop()
        assert consumer._stop_event.is_set()  # pyright: ignore[reportPrivateUsage]

    def test_is_running_false_when_not_started(self) -> None:
        mock_connection = MagicMock()
        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)
        assert consumer.is_running is False


class TestOnMessage:
    """Tests for the internal message callback."""

    def test_deserializes_and_calls_callback(self) -> None:
        received: list[CommandMessage] = []
        mock_connection = MagicMock()
        consumer = CommandConsumer(mock_connection, _make_settings(), received.append)

        channel = MagicMock()
        deliver = _make_deliver()
        properties = MagicMock(spec=pika.spec.BasicProperties)
        body = _make_body()

        consumer._on_message(channel, deliver, properties, body)  # pyright: ignore[reportPrivateUsage]

        assert len(received) == 1
        assert received[0].app_id == "abc123"
        assert received[0].command == CmdName.START

    def test_acks_on_success(self) -> None:
        mock_connection = MagicMock()
        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)

        channel = MagicMock()
        deliver = _make_deliver(delivery_tag=42)
        consumer._on_message(channel, deliver, MagicMock(), _make_body())  # pyright: ignore[reportPrivateUsage]

        channel.basic_ack.assert_called_once_with(delivery_tag=42)

    def test_nacks_on_deserialization_error(self) -> None:
        mock_connection = MagicMock()
        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)

        channel = MagicMock()
        deliver = _make_deliver(delivery_tag=7)
        consumer._on_message(channel, deliver, MagicMock(), b"not json")  # pyright: ignore[reportPrivateUsage]

        channel.basic_nack.assert_called_once_with(delivery_tag=7, requeue=False)
        channel.basic_ack.assert_not_called()

    def test_nacks_on_callback_error(self) -> None:
        def failing_callback(message: CommandMessage) -> None:
            raise ValueError("processing failed")

        mock_connection = MagicMock()
        consumer = CommandConsumer(mock_connection, _make_settings(), failing_callback)

        channel = MagicMock()
        deliver = _make_deliver(delivery_tag=9)
        consumer._on_message(channel, deliver, MagicMock(), _make_body())  # pyright: ignore[reportPrivateUsage]

        channel.basic_nack.assert_called_once_with(delivery_tag=9, requeue=False)
        channel.basic_ack.assert_not_called()

    def test_nacks_on_invalid_command(self) -> None:
        mock_connection = MagicMock()
        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)

        channel = MagicMock()
        deliver = _make_deliver(delivery_tag=11)
        body = _make_body(command=99)
        consumer._on_message(channel, deliver, MagicMock(), body)  # pyright: ignore[reportPrivateUsage]

        channel.basic_nack.assert_called_once_with(delivery_tag=11, requeue=False)


class TestConsumeLoop:
    """Tests for the consume loop behaviour."""

    @patch("data_collector.messaging.consumer.declare_topology")
    def test_declares_topology_and_consumes(self, mock_declare: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.ensure_connected.return_value = mock_channel

        settings = _make_settings()
        consumer = CommandConsumer(mock_connection, settings, lambda message: None)

        # start_consuming sets stop event then raises, so loop exits on the except branch
        def stop_and_raise() -> None:
            consumer._stop_event.set()  # pyright: ignore[reportPrivateUsage]
            raise pika.exceptions.AMQPError("test stop")

        mock_channel.start_consuming.side_effect = stop_and_raise

        consumer._consume_loop()  # pyright: ignore[reportPrivateUsage]

        mock_declare.assert_called_once_with(mock_channel, "dc_test")
        assert mock_channel.basic_consume.call_count == 2

    @patch("data_collector.messaging.consumer.declare_topology")
    def test_consumes_from_both_queues(self, mock_declare: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.ensure_connected.return_value = mock_channel

        consumer = CommandConsumer(mock_connection, _make_settings(), lambda message: None)

        def stop_and_raise() -> None:
            consumer._stop_event.set()  # pyright: ignore[reportPrivateUsage]
            raise pika.exceptions.AMQPError("stop")

        mock_channel.start_consuming.side_effect = stop_and_raise

        consumer._consume_loop()  # pyright: ignore[reportPrivateUsage]

        queue_names = [call_item.kwargs["queue"] for call_item in mock_channel.basic_consume.call_args_list]
        assert "dc_test" in queue_names
        assert "dc_test_broadcast" in queue_names
