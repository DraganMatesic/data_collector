"""Tests for CommandPublisher."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

from data_collector.enums.commands import CmdName
from data_collector.messaging.models import CommandMessage
from data_collector.messaging.publisher import CommandPublisher
from data_collector.messaging.topology import BROADCAST_ROUTING_KEY, EXCHANGE_NAME


def _make_message(**overrides: object) -> CommandMessage:
    defaults: dict[str, object] = {
        "app_id": "abc123",
        "command": CmdName.START,
        "issued_by": "admin@example.com",
        "timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC),
    }
    defaults.update(overrides)
    return CommandMessage(**defaults)  # type: ignore[arg-type]


def _make_publisher() -> tuple[CommandPublisher, MagicMock]:
    mock_connection = MagicMock()
    mock_channel = MagicMock()
    mock_connection.ensure_connected.return_value = mock_channel
    publisher = CommandPublisher(mock_connection)
    return publisher, mock_channel


class TestPublish:
    """Tests for the publish method."""

    def test_publishes_to_exchange(self) -> None:
        publisher, mock_channel = _make_publisher()
        message = _make_message()
        publisher.publish(message, routing_key="dc_manager_eu")

        mock_channel.basic_publish.assert_called_once()
        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs.kwargs["exchange"] == EXCHANGE_NAME
        assert call_kwargs.kwargs["routing_key"] == "dc_manager_eu"

    def test_persistent_delivery(self) -> None:
        publisher, mock_channel = _make_publisher()
        publisher.publish(_make_message(), routing_key="test")

        call_kwargs = mock_channel.basic_publish.call_args
        properties = call_kwargs.kwargs["properties"]
        assert properties.delivery_mode == 2

    def test_content_type_json(self) -> None:
        publisher, mock_channel = _make_publisher()
        publisher.publish(_make_message(), routing_key="test")

        call_kwargs = mock_channel.basic_publish.call_args
        properties = call_kwargs.kwargs["properties"]
        assert properties.content_type == "application/json"

    def test_json_body_format(self) -> None:
        publisher, mock_channel = _make_publisher()
        message = _make_message(command=CmdName.STOP, args={"key": "value"})
        publisher.publish(message, routing_key="test")

        call_kwargs = mock_channel.basic_publish.call_args
        body = json.loads(call_kwargs.kwargs["body"])
        assert body["app_id"] == "abc123"
        assert body["command"] == 2
        assert body["args"] == {"key": "value"}

    def test_calls_ensure_connected(self) -> None:
        mock_connection = MagicMock()
        mock_connection.ensure_connected.return_value = MagicMock()
        publisher = CommandPublisher(mock_connection)
        publisher.publish(_make_message(), routing_key="test")

        mock_connection.ensure_connected.assert_called_once()


class TestPublishToManager:
    """Tests for the publish_to_manager convenience method."""

    def test_routing_key_is_manager_queue(self) -> None:
        publisher, mock_channel = _make_publisher()
        publisher.publish_to_manager(_make_message(), manager_queue="dc_manager_eu")

        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs.kwargs["routing_key"] == "dc_manager_eu"


class TestBroadcast:
    """Tests for the broadcast convenience method."""

    def test_routing_key_is_broadcast(self) -> None:
        publisher, mock_channel = _make_publisher()
        publisher.broadcast(_make_message())

        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs.kwargs["routing_key"] == BROADCAST_ROUTING_KEY
