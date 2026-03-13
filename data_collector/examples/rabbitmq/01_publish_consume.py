"""Publish a command and consume it via RabbitMQ in a single process.

Demonstrates:
    - LoggingService with structured logging (no database required)
    - RabbitMQSettings construction from DC_RABBIT_* environment variables
    - RabbitMQConnection connect/close lifecycle with try/finally
    - declare_topology() for exchange and queue setup
    - CommandMessage construction with CmdName enum and optional args
    - CommandPublisher.publish_to_manager() for targeted delivery
    - CommandConsumer with callback dispatch in a background daemon thread
    - Consumer start()/stop() lifecycle and is_running property
    - is_healthy() connection health check
    - Clean shutdown of consumer, connections, and logging service

Requires:
    RabbitMQ running on localhost:5672 with guest/guest credentials
    (or set DC_RABBIT_HOST, DC_RABBIT_PORT, DC_RABBIT_USERNAME,
    DC_RABBIT_PASSWORD environment variables).

Run:
    python -m data_collector.examples run rabbitmq/01_publish_consume
"""

from __future__ import annotations

import threading
import time
import uuid

from data_collector.enums.commands import CmdName
from data_collector.messaging import (
    CommandConsumer,
    CommandMessage,
    CommandPublisher,
    RabbitMQConnection,
    declare_topology,
)
from data_collector.settings.main import LogSettings
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.utilities.log.main import LoggingService


def main() -> None:
    """Publish a START command and consume it from a per-manager queue."""
    log_settings = LogSettings(log_to_db=False, log_level=10, log_error_file="error.log")
    logging_service = LoggingService(
        logger_name="examples.rabbitmq.publish_consume",
        settings=log_settings,
    )
    logger = logging_service.configure_logger()
    logger = logger.bind(runtime=uuid.uuid4().hex)

    settings = RabbitMQSettings()
    publisher_connection = RabbitMQConnection(settings)
    consumer_connection = RabbitMQConnection(settings)
    consumer: CommandConsumer | None = None

    try:
        # --- Connect and declare topology ---
        print("=== RabbitMQ Publish/Consume Example ===\n")
        publisher_connection.connect()
        logger.info(
            "Publisher connected",
            host=settings.rabbit_host,
            port=settings.rabbit_port,
            healthy=publisher_connection.is_healthy(),
        )

        channel = publisher_connection.ensure_connected()
        declare_topology(channel, settings.rabbit_queue)
        logger.info("Topology declared", queue=settings.rabbit_queue)

        # --- Start consumer ---
        message_received_signal = threading.Event()
        received_messages: list[CommandMessage] = []

        def handle_command(message: CommandMessage) -> None:
            received_messages.append(message)
            logger.info(
                "Received command",
                app_id=message.app_id[:16] + "...",
                command=message.command.name,
                command_value=int(message.command),
                issued_by=message.issued_by,
                timestamp=message.timestamp.isoformat(),
                args=message.args,
            )
            message_received_signal.set()

        consumer = CommandConsumer(consumer_connection, settings, handle_command)
        consumer.start()
        time.sleep(0.5)
        logger.info("Consumer running", is_running=consumer.is_running)

        # --- Publish command ---
        print("\n--- Publishing START command ---")
        command_message = CommandMessage(
            app_id="a" * 64,
            command=CmdName.START,
            issued_by="admin@example.com",
            args={"priority": "high"},
        )
        publisher = CommandPublisher(publisher_connection)
        publisher.publish_to_manager(command_message, settings.rabbit_queue)
        logger.info(
            "Published command",
            command=command_message.command.name,
            routing_key=settings.rabbit_queue,
        )

        # --- Wait for delivery ---
        print("\n--- Waiting for consumer ---")
        delivered = message_received_signal.wait(timeout=5.0)
        if delivered:
            logger.info("Message delivered successfully", total_received=len(received_messages))
        else:
            logger.warning("Timed out waiting for message delivery")

    finally:
        print("\n--- Cleanup ---")
        if consumer is not None:
            consumer.stop()
        publisher_connection.close()
        consumer_connection.close()
        logging_service.stop()
        print("Cleanup complete.")


if __name__ == "__main__":
    main()
