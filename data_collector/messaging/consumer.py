"""Command consumer for RabbitMQ command queues with background thread consumption."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

import pika.exceptions

from data_collector.messaging.connection import RabbitMQConnection
from data_collector.messaging.models import CommandMessage
from data_collector.messaging.topology import declare_topology
from data_collector.settings.rabbitmq import RabbitMQSettings

logger = logging.getLogger(__name__)

CommandCallback = Callable[[CommandMessage], None]
"""Type alias for the user-provided callback invoked on each received command."""


class CommandConsumer:
    """Consumes command messages from RabbitMQ in a background daemon thread.

    Listens on both the per-manager queue and the per-manager broadcast
    queue. Each received message is deserialized into a ``CommandMessage``
    and dispatched to the user-provided callback. Messages that fail
    deserialization or cause callback errors are negatively acknowledged
    without requeue to prevent poison-message loops.

    Args:
        connection: RabbitMQConnection instance for channel access.
        settings: RabbitMQSettings for queue name configuration.
        callback: Function invoked with each received ``CommandMessage``.
    """

    def __init__(
        self,
        connection: RabbitMQConnection,
        settings: RabbitMQSettings,
        callback: CommandCallback,
    ) -> None:
        self._connection = connection
        self._settings = settings
        self._callback = callback
        self._stop_event = threading.Event()
        self._consumer_thread: threading.Thread | None = None

    def start(self) -> None:
        """Start consuming messages in a background daemon thread.

        The consumer declares the command topology on startup, then
        enters ``start_consuming()``. On connection drops, it
        automatically reconnects and resumes consumption.

        Raises:
            RuntimeError: If the consumer is already running.
        """
        if self._consumer_thread is not None and self._consumer_thread.is_alive():
            raise RuntimeError("Consumer is already running")

        self._stop_event.clear()
        self._consumer_thread = threading.Thread(
            target=self._consume_loop,
            daemon=True,
            name=f"rabbitmq-consumer-{self._settings.rabbit_queue}",
        )
        self._consumer_thread.start()
        logger.info("Command consumer started for queue '%s'", self._settings.rabbit_queue)

    def stop(self, timeout: float = 10.0) -> None:
        """Signal the consumer to stop and wait for the thread to exit.

        Args:
            timeout: Maximum seconds to wait for thread shutdown.
        """
        self._stop_event.set()

        try:
            connection = self._connection.connection
            if connection.is_open:
                connection.add_callback_threadsafe(self._connection.stop_consuming)  # pyright: ignore[reportUnknownMemberType]
        except (ConnectionError, pika.exceptions.AMQPError):
            logger.debug("Could not inject stop callback (connection closed)", exc_info=True)

        if self._consumer_thread is not None:
            self._consumer_thread.join(timeout=timeout)
            if self._consumer_thread.is_alive():
                logger.warning("Consumer thread did not exit within %.1f seconds", timeout)
            self._consumer_thread = None

        logger.info("Command consumer stopped")

    @property
    def is_running(self) -> bool:
        """Whether the consumer thread is alive."""
        return self._consumer_thread is not None and self._consumer_thread.is_alive()

    def _consume_loop(self) -> None:
        """Main consumer loop running in the daemon thread.

        Declares topology, subscribes to both queues, and enters
        ``start_consuming()``. On connection errors, reconnects with
        backoff unless the stop event is set.
        """
        base_delay = self._settings.rabbit_reconnect_base_delay
        max_delay = self._settings.rabbit_reconnect_max_delay

        while not self._stop_event.is_set():
            try:
                channel = self._connection.ensure_connected()
                declare_topology(channel, self._settings.rabbit_queue)

                queue_name = self._settings.rabbit_queue
                broadcast_queue_name = f"{queue_name}_broadcast"

                channel.basic_consume(  # pyright: ignore[reportUnknownMemberType]
                    queue=queue_name,
                    on_message_callback=self._on_message,
                )
                channel.basic_consume(  # pyright: ignore[reportUnknownMemberType]
                    queue=broadcast_queue_name,
                    on_message_callback=self._on_message,
                )

                logger.info(
                    "Consuming from queues '%s' and '%s'",
                    queue_name,
                    broadcast_queue_name,
                )
                channel.start_consuming()

            except pika.exceptions.AMQPError as error:
                if self._stop_event.is_set():
                    break
                logger.warning("Consumer connection error: %s. Reconnecting...", error)
                delay = min(base_delay, max_delay)
                base_delay = min(base_delay * 2, max_delay)
                self._stop_event.wait(timeout=delay)

            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception("Unexpected error in consumer loop. Reconnecting...")
                self._stop_event.wait(timeout=max_delay)

    def _on_message(
        self,
        channel: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        """Internal pika callback for received messages.

        Deserializes the message body, invokes the user callback, and
        acknowledges the message. On any error, negatively acknowledges
        without requeue to prevent poison-message loops.

        Args:
            channel: The pika channel the message was received on.
            method: Delivery metadata including delivery tag and routing key.
            properties: Message properties.
            body: Raw message body bytes.
        """
        try:
            message = CommandMessage.from_json_bytes(body)
            self._callback(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.debug(
                "Processed command %s for app_id=%s from routing_key='%s'",
                message.command.name,
                message.app_id,
                method.routing_key,
            )
        except Exception:
            logger.exception(
                "Failed to process message from routing_key='%s', nacking without requeue",
                method.routing_key,
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
