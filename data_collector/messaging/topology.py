"""RabbitMQ command exchange and queue topology declaration."""

from __future__ import annotations

import logging

import pika.adapters.blocking_connection

logger = logging.getLogger(__name__)

EXCHANGE_NAME: str = "dc_commands"
"""Direct exchange for command distribution to manager instances."""

BROADCAST_ROUTING_KEY: str = "all"
"""Routing key used for broadcast commands sent to all managers."""


def declare_exchange(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    """Declare the ``dc_commands`` direct exchange.

    Args:
        channel: Open pika channel.
    """
    channel.exchange_declare(  # pyright: ignore[reportUnknownMemberType]
        exchange=EXCHANGE_NAME,
        exchange_type="direct",
        durable=True,
    )
    logger.debug("Declared exchange '%s' (direct, durable)", EXCHANGE_NAME)


def declare_queue_with_binding(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queue_name: str,
    routing_key: str,
) -> None:
    """Declare a durable queue and bind it to the command exchange.

    Args:
        channel: Open pika channel.
        queue_name: Name of the queue to declare.
        routing_key: Routing key for the binding to ``dc_commands``.
    """
    channel.queue_declare(queue=queue_name, durable=True)  # pyright: ignore[reportUnknownMemberType]
    channel.queue_bind(  # pyright: ignore[reportUnknownMemberType]
        queue=queue_name,
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
    )
    logger.debug(
        "Declared queue '%s' bound to '%s' with routing_key='%s'",
        queue_name,
        EXCHANGE_NAME,
        routing_key,
    )


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel, queue_name: str) -> None:
    """Declare the full command queue topology for a manager instance.

    Creates the ``dc_commands`` direct exchange, a per-manager queue
    (routing_key = ``queue_name``), and a per-manager broadcast queue
    (routing_key = ``"all"``). Each manager instance gets its own
    broadcast queue to ensure every manager receives broadcast commands.

    Args:
        channel: Open pika channel.
        queue_name: Per-manager queue name (from ``RabbitMQSettings.rabbit_queue``).
    """
    declare_exchange(channel)
    declare_queue_with_binding(channel, queue_name, queue_name)

    broadcast_queue_name = f"{queue_name}_broadcast"
    declare_queue_with_binding(channel, broadcast_queue_name, BROADCAST_ROUTING_KEY)

    logger.info(
        "Topology declared: exchange='%s', queues=['%s', '%s']",
        EXCHANGE_NAME,
        queue_name,
        broadcast_queue_name,
    )
