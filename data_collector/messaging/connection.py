"""pika BlockingConnection wrapper with automatic reconnection and health checks."""

from __future__ import annotations

import logging
import threading
import time

import pika
import pika.adapters.blocking_connection

from data_collector.settings.rabbitmq import RabbitMQSettings

logger = logging.getLogger(__name__)


class RabbitMQConnection:
    """Manages a pika BlockingConnection with thread-safe automatic reconnection.

    Provides ``connect()``, ``close()``, ``ensure_connected()``, and
    ``is_healthy()`` for robust RabbitMQ communication. Every public method
    that requires an open channel calls ``ensure_connected()`` internally,
    which transparently reconnects with bounded exponential backoff when
    the connection or channel has dropped.

    Args:
        settings: RabbitMQSettings instance with connection parameters.
    """

    def __init__(self, settings: RabbitMQSettings) -> None:
        self._settings = settings
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None
        self._lock = threading.Lock()

    @property
    def settings(self) -> RabbitMQSettings:
        """Return the settings used for this connection."""
        return self._settings

    def connect(self) -> None:
        """Establish a new connection and channel to RabbitMQ.

        Builds ``pika.ConnectionParameters`` from settings, opens a
        ``BlockingConnection``, creates a channel, and sets the prefetch
        count.

        Raises:
            pika.exceptions.AMQPConnectionError: If the broker is unreachable.
        """
        credentials = pika.PlainCredentials(
            self._settings.rabbit_username,
            self._settings.rabbit_password,
        )
        parameters = pika.ConnectionParameters(
            host=self._settings.rabbit_host,
            port=self._settings.rabbit_port,
            credentials=credentials,
            heartbeat=self._settings.rabbit_heartbeat,
            blocked_connection_timeout=self._settings.rabbit_connection_timeout,
        )
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=self._settings.rabbit_prefetch)  # pyright: ignore[reportOptionalMemberAccess]
        logger.info(
            "Connected to RabbitMQ at %s:%d",
            self._settings.rabbit_host,
            self._settings.rabbit_port,
        )

    def close(self) -> None:
        """Gracefully close the channel and connection.

        Swallows any exceptions during shutdown to allow clean teardown
        even when the broker has already disconnected.
        """
        try:
            if self._channel is not None and self._channel.is_open:  # pyright: ignore[reportUnknownMemberType]
                self._channel.close()  # pyright: ignore[reportUnknownMemberType]
        except Exception:
            logger.debug("Exception closing channel (ignored)", exc_info=True)
        finally:
            self._channel = None

        try:
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception:
            logger.debug("Exception closing connection (ignored)", exc_info=True)
        finally:
            self._connection = None

    def ensure_connected(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """Return an open channel, reconnecting transparently if necessary.

        Thread-safe: acquires an internal lock before checking and
        potentially re-establishing the connection.

        Returns:
            An open pika channel ready for use.

        Raises:
            ConnectionError: If reconnection fails after all retry attempts.
        """
        with self._lock:
            if self._is_open():
                assert self._channel is not None  # noqa: S101
                return self._channel
            return self._reconnect()

    def is_healthy(self) -> bool:
        """Check whether the connection and channel are both open.

        Returns:
            True if both are open and usable, False otherwise.
        """
        return self._is_open()

    @property
    def channel(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """Return the current channel, reconnecting if needed."""
        return self.ensure_connected()

    @property
    def connection(self) -> pika.BlockingConnection:
        """Return the current connection.

        Raises:
            ConnectionError: If no connection is established.
        """
        if self._connection is None or not self._connection.is_open:
            raise ConnectionError("No active RabbitMQ connection")
        return self._connection

    def stop_consuming(self) -> None:
        """Stop consuming on the current channel if open.

        Intended to be called via ``connection.add_callback_threadsafe()``
        from another thread to cleanly shut down a consumer.
        """
        try:
            if self._channel is not None and self._channel.is_open:  # pyright: ignore[reportUnknownMemberType]
                self._channel.stop_consuming()  # pyright: ignore[reportUnknownMemberType]
        except Exception:
            logger.debug("Exception stopping consuming (ignored)", exc_info=True)

    def _is_open(self) -> bool:
        """Check if both connection and channel are open."""
        if self._connection is None or not self._connection.is_open:
            return False
        if self._channel is None:
            return False
        return bool(self._channel.is_open)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]

    def _reconnect(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """Attempt reconnection with bounded exponential backoff.

        Returns:
            An open pika channel after successful reconnection.

        Raises:
            ConnectionError: If all retry attempts are exhausted.
        """
        max_attempts = self._settings.rabbit_reconnect_max_attempts
        base_delay = self._settings.rabbit_reconnect_base_delay
        max_delay = self._settings.rabbit_reconnect_max_delay
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                self.close()
                self.connect()
                if attempt > 1:
                    logger.info("Reconnected to RabbitMQ on attempt %d/%d", attempt, max_attempts)
                assert self._channel is not None  # noqa: S101
                return self._channel
            except Exception as error:
                last_error = error
                logger.warning(
                    "RabbitMQ reconnection attempt %d/%d failed: %s",
                    attempt,
                    max_attempts,
                    error,
                )
                if attempt < max_attempts:
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    time.sleep(delay)

        raise ConnectionError(
            f"Failed to connect to RabbitMQ after {max_attempts} attempts: {last_error}"
        )
