"""Command publisher for the RabbitMQ command exchange."""

from __future__ import annotations

import logging

import pika

from data_collector.messaging.connection import RabbitMQConnection
from data_collector.messaging.models import CommandMessage
from data_collector.messaging.topology import BROADCAST_ROUTING_KEY, EXCHANGE_NAME

logger = logging.getLogger(__name__)


class CommandPublisher:
    """Publishes command messages to the ``dc_commands`` exchange.

    Messages are published with ``delivery_mode=2`` (persistent) to
    survive RabbitMQ restarts. The publisher calls ``ensure_connected()``
    before each publish to handle transient connection drops transparently.

    Args:
        connection: RabbitMQConnection instance for channel access.
    """

    def __init__(self, connection: RabbitMQConnection) -> None:
        self._connection = connection

    def publish(self, message: CommandMessage, routing_key: str) -> None:
        """Publish a command message to the ``dc_commands`` exchange.

        Args:
            message: The command message to publish.
            routing_key: Target routing key (manager queue name or ``"all"``
                for broadcast).
        """
        channel = self._connection.ensure_connected()
        body = message.to_json_bytes()
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        logger.debug(
            "Published command %s for app_id=%s to routing_key='%s'",
            message.command.name,
            message.app_id,
            routing_key,
        )

    def publish_to_manager(self, message: CommandMessage, manager_queue: str) -> None:
        """Publish a command to a specific manager instance.

        Args:
            message: The command message to publish.
            manager_queue: Target manager's queue name.
        """
        self.publish(message, routing_key=manager_queue)

    def broadcast(self, message: CommandMessage) -> None:
        """Publish a command to all manager instances via broadcast.

        Args:
            message: The command message to broadcast.
        """
        self.publish(message, routing_key=BROADCAST_ROUTING_KEY)
