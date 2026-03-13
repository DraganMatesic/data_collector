"""Tests for RabbitMQ topology declaration."""

from __future__ import annotations

from unittest.mock import MagicMock, call

from data_collector.messaging.topology import (
    BROADCAST_ROUTING_KEY,
    EXCHANGE_NAME,
    declare_exchange,
    declare_queue_with_binding,
    declare_topology,
)


class TestDeclareExchange:
    """Tests for exchange declaration."""

    def test_declares_direct_durable_exchange(self) -> None:
        channel = MagicMock()
        declare_exchange(channel)
        channel.exchange_declare.assert_called_once_with(
            exchange=EXCHANGE_NAME,
            exchange_type="direct",
            durable=True,
        )

    def test_exchange_name_constant(self) -> None:
        assert EXCHANGE_NAME == "dc_commands"


class TestDeclareQueueWithBinding:
    """Tests for queue declaration and binding."""

    def test_declares_durable_queue(self) -> None:
        channel = MagicMock()
        declare_queue_with_binding(channel, "my_queue", "my_key")
        channel.queue_declare.assert_called_once_with(queue="my_queue", durable=True)

    def test_binds_to_exchange(self) -> None:
        channel = MagicMock()
        declare_queue_with_binding(channel, "my_queue", "my_key")
        channel.queue_bind.assert_called_once_with(
            queue="my_queue",
            exchange=EXCHANGE_NAME,
            routing_key="my_key",
        )


class TestDeclareTopology:
    """Tests for full topology declaration."""

    def test_declares_exchange_and_two_queues(self) -> None:
        channel = MagicMock()
        declare_topology(channel, "dc_manager")

        channel.exchange_declare.assert_called_once()
        assert channel.queue_declare.call_count == 2
        assert channel.queue_bind.call_count == 2

    def test_per_manager_queue_binding(self) -> None:
        channel = MagicMock()
        declare_topology(channel, "dc_manager_eu")

        channel.queue_declare.assert_any_call(queue="dc_manager_eu", durable=True)
        channel.queue_bind.assert_any_call(
            queue="dc_manager_eu",
            exchange=EXCHANGE_NAME,
            routing_key="dc_manager_eu",
        )

    def test_broadcast_queue_naming(self) -> None:
        channel = MagicMock()
        declare_topology(channel, "dc_manager_eu")

        channel.queue_declare.assert_any_call(queue="dc_manager_eu_broadcast", durable=True)
        channel.queue_bind.assert_any_call(
            queue="dc_manager_eu_broadcast",
            exchange=EXCHANGE_NAME,
            routing_key=BROADCAST_ROUTING_KEY,
        )

    def test_broadcast_routing_key_constant(self) -> None:
        assert BROADCAST_ROUTING_KEY == "all"

    def test_declaration_order(self) -> None:
        channel = MagicMock()
        declare_topology(channel, "dc_mgr")

        expected_calls = [
            call.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True),
            call.queue_declare(queue="dc_mgr", durable=True),
            call.queue_bind(queue="dc_mgr", exchange=EXCHANGE_NAME, routing_key="dc_mgr"),
            call.queue_declare(queue="dc_mgr_broadcast", durable=True),
            call.queue_bind(queue="dc_mgr_broadcast", exchange=EXCHANGE_NAME, routing_key="all"),
        ]
        assert channel.mock_calls == expected_calls
