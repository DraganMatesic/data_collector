"""Tests for DramatiqBroker."""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.dramatiq.topic.base import (
    DEAD_LETTERS_QUEUE,
    OCR_TOPIC_EXCHANGE,
    UNROUTABLE_EXCHANGE,
    TopicExchangeQueue,
)
from data_collector.settings.dramatiq import DramatiqSettings
from data_collector.settings.rabbitmq import RabbitMQSettings


def _make_settings(**overrides: object) -> RabbitMQSettings:
    defaults: dict[str, object] = {
        "reconnect_max_attempts": 1,
        "reconnect_base_delay": 0,
        "reconnect_max_delay": 0,
    }
    defaults.update(overrides)
    return RabbitMQSettings(**defaults)  # type: ignore[arg-type]


class TestBrokerCreation:
    """Tests for DramatiqBroker construction."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_creates_broker_and_sets_global(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)
        mock_rabbitmq_broker.assert_called_once()
        mock_set_broker.assert_called_once_with(broker.broker)

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_stores_settings(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        dramatiq_settings = DramatiqSettings(workers=8)
        broker = DramatiqBroker(_make_settings(), dramatiq_settings=dramatiq_settings, load=False)
        assert broker.dramatiq_settings.workers == 8

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_defaults_dramatiq_settings(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)
        assert broker.dramatiq_settings.workers == 4
        assert broker.dramatiq_settings.max_retries == 3


class TestActorExists:
    """Tests for AST-based actor validation."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_finds_existing_function(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock, tmp_path: Path
    ) -> None:
        # Create a temp module with a function
        module_file = tmp_path / "test_module.py"
        module_file.write_text(textwrap.dedent("""
            def my_actor(event_id):
                pass
        """))

        broker = DramatiqBroker(_make_settings(), load=False)

        with patch("data_collector.dramatiq.broker.importlib.util.find_spec") as mock_find_spec:
            mock_spec = MagicMock()
            mock_spec.origin = str(module_file)
            mock_find_spec.return_value = mock_spec

            assert broker._actor_exists("some.module", "my_actor") is True

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_returns_false_for_missing_function(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock, tmp_path: Path
    ) -> None:
        module_file = tmp_path / "test_module.py"
        module_file.write_text("def other_function(): pass")

        broker = DramatiqBroker(_make_settings(), load=False)

        with patch("data_collector.dramatiq.broker.importlib.util.find_spec") as mock_find_spec:
            mock_spec = MagicMock()
            mock_spec.origin = str(module_file)
            mock_find_spec.return_value = mock_spec

            assert broker._actor_exists("some.module", "nonexistent_actor") is False

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_returns_false_for_missing_module(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        with patch("data_collector.dramatiq.broker.importlib.util.find_spec", return_value=None):
            assert broker._actor_exists("nonexistent.module", "my_actor") is False


class TestCreateMessage:
    """Tests for message creation."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    @patch("data_collector.dramatiq.broker.dramatiq.Message")
    def test_creates_message_with_correct_params(
        self,
        mock_message_class: MagicMock,
        mock_rabbitmq_broker: MagicMock,
        mock_set_broker: MagicMock,
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)
        broker.create_message(
            queue_name="dc_test",
            actor_name="test_actor",
            args=(42,),
            kwargs={"key": "value"},
        )

        mock_message_class.assert_called_once_with(
            queue_name="dc_test",
            actor_name="test_actor",
            args=(42,),
            kwargs={"key": "value"},
            options={},
        )


class TestPublish:
    """Tests for message publishing."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_publish_uses_persistent_delivery(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        mock_message = MagicMock()
        mock_message.queue_name = "dc_test"
        mock_message.actor_name = "test_actor"
        mock_message.encode.return_value = b"encoded_message"

        broker.publish(mock_message, exchange_name="dc_exchange", routing_key="test.key")

        mock_channel.basic_publish.assert_called_once()
        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs[1]["exchange"] == "dc_exchange"
        assert call_kwargs[1]["routing_key"] == "test.key"
        assert call_kwargs[1]["properties"].delivery_mode == 2

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_publish_defaults_routing_key_to_queue_name(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        mock_message = MagicMock()
        mock_message.queue_name = "dc_my_queue"
        mock_message.actor_name = "my_actor"
        mock_message.encode.return_value = b"encoded"

        broker.publish(mock_message)

        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs[1]["routing_key"] == "dc_my_queue"


class TestDeclareExchanges:
    """Tests for exchange declaration."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_declares_all_exchanges(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        exchanges = [UNROUTABLE_EXCHANGE, OCR_TOPIC_EXCHANGE]
        with patch.object(broker, "_get_all_exchanges", return_value=exchanges):
            broker._declare_exchanges()

        assert mock_channel.exchange_declare.call_count == 2


class TestSyncBindings:
    """Tests for binding synchronization."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_creates_missing_binding(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        test_queue = TopicExchangeQueue(
            name="dc_test_queue",
            actor_name="test_actor",
            exchange_name="dc_test_exchange",
            routing_key="test.routing.key",
        )

        with (
            patch.object(broker, "_get_all_queues", return_value=[test_queue]),
            patch.object(broker, "_get_exchange_bindings", return_value=[]),
            patch.object(broker, "_actor_exists", return_value=True),
        ):
            broker._sync_bindings()

        mock_channel.queue_bind.assert_called_once_with(
            queue="dc_test_queue",
            exchange="dc_test_exchange",
            routing_key="test.routing.key",
        )

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_removes_stale_binding(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        existing_bindings = [{"destination": "dc_old_queue", "routing_key": "old.key"}]

        with (
            patch.object(broker, "_get_all_queues", return_value=[]),
            patch.object(broker, "_get_exchange_bindings", return_value=existing_bindings),
        ):
            # No queues defined for any exchange, but we need at least one
            # TopicExchangeQueue to trigger the exchange iteration.
            # Since _get_all_queues returns empty, _sync_bindings won't iterate.
            # Let's test directly with a queue that has a different exchange.
            pass

        # The sync_bindings only iterates exchanges that have queues defined.
        # Test with a queue that has a stale binding.
        stale_queue = TopicExchangeQueue(
            name="dc_new_queue",
            actor_name="new_actor",
            exchange_name="dc_test_exchange",
            routing_key="new.key",
        )

        with (
            patch.object(broker, "_get_all_queues", return_value=[stale_queue]),
            patch.object(broker, "_get_exchange_bindings", return_value=existing_bindings),
            patch.object(broker, "_actor_exists", return_value=True),
        ):
            broker._sync_bindings()

        # Stale binding should be removed
        mock_channel.queue_unbind.assert_called_once_with(
            queue="dc_old_queue",
            exchange="dc_test_exchange",
            routing_key="old.key",
        )
        # New binding should be created
        mock_channel.queue_bind.assert_called_once()

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_skips_binding_when_actor_not_found(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        test_queue = TopicExchangeQueue(
            name="dc_test_queue",
            actor_name="missing_actor",
            exchange_name="dc_test_exchange",
            routing_key="test.key",
            actor_path="some.module",
        )

        with (
            patch.object(broker, "_get_all_queues", return_value=[test_queue]),
            patch.object(broker, "_get_exchange_bindings", return_value=[]),
            patch.object(broker, "_actor_exists", return_value=False),
        ):
            broker._sync_bindings()

        mock_channel.queue_bind.assert_not_called()


class TestCleanDeadQueues:
    """Tests for orphan queue cleanup."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_removes_orphan_dc_queues(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        code_queues = [DEAD_LETTERS_QUEUE]
        broker_queues = {"dc_dead_letters", "dc_orphan_queue", "other_app_queue"}

        with (
            patch.object(broker, "_get_all_queues", return_value=code_queues),
            patch.object(broker, "_get_broker_queues", return_value=broker_queues),
        ):
            broker._clean_dead_queues()

        # Should delete dc_orphan_queue but NOT other_app_queue (no dc_ prefix)
        mock_channel.queue_delete.assert_called_once_with(queue="dc_orphan_queue")

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_preserves_non_dc_queues(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)

        mock_channel = MagicMock()
        broker._management_connection = MagicMock()
        broker._management_connection.ensure_connected.return_value = mock_channel

        broker_queues = {"celery_queue", "other_service_queue"}

        with (
            patch.object(broker, "_get_all_queues", return_value=[]),
            patch.object(broker, "_get_broker_queues", return_value=broker_queues),
        ):
            broker._clean_dead_queues()

        mock_channel.queue_delete.assert_not_called()


class TestClose:
    """Tests for broker lifecycle."""

    @patch("data_collector.dramatiq.broker.dramatiq.set_broker")
    @patch("data_collector.dramatiq.broker.dramatiq.brokers.rabbitmq.RabbitmqBroker")
    def test_close_closes_management_connection(
        self, mock_rabbitmq_broker: MagicMock, mock_set_broker: MagicMock
    ) -> None:
        broker = DramatiqBroker(_make_settings(), load=False)
        broker._management_connection = MagicMock()
        broker.close()
        broker._management_connection.close.assert_called_once()
