"""Broadcast a command to multiple simulated manager instances.

Demonstrates:
    - LoggingService with structured logging (no database required)
    - EXCHANGE_NAME and BROADCAST_ROUTING_KEY topology constants
    - declare_topology() with multiple queue names (two simulated managers)
    - CommandPublisher.broadcast() for fan-out to all managers
    - Per-manager broadcast queue naming convention ({queue}_broadcast)
    - Multiple CommandConsumer instances on different queues
    - CommandMessage.to_json_bytes() / from_json_bytes() serialization round-trip

Requires:
    RabbitMQ running on localhost:5672 with guest/guest credentials
    (or set DC_RABBIT_* environment variables).

Run:
    python -m data_collector.examples run rabbitmq/02_broadcast
"""

from __future__ import annotations

import threading
import time
import uuid

from data_collector.enums.commands import CmdName
from data_collector.messaging import (
    BROADCAST_ROUTING_KEY,
    EXCHANGE_NAME,
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
    """Broadcast a STOP command and verify both managers receive it."""
    log_settings = LogSettings(log_to_db=False, log_level=10, log_error_file="error.log")
    logging_service = LoggingService(
        logger_name="examples.rabbitmq.broadcast",
        settings=log_settings,
    )
    logger = logging_service.configure_logger()
    logger = logger.bind(runtime=uuid.uuid4().hex)

    manager_queue_alpha = "dc_example_alpha"
    manager_queue_beta = "dc_example_beta"

    settings = RabbitMQSettings()
    publisher_connection = RabbitMQConnection(settings)
    alpha_connection = RabbitMQConnection(settings)
    beta_connection = RabbitMQConnection(settings)
    alpha_consumer: CommandConsumer | None = None
    beta_consumer: CommandConsumer | None = None

    try:
        # --- Connect and declare topology for both managers ---
        print("=== RabbitMQ Broadcast Example ===\n")
        publisher_connection.connect()

        channel = publisher_connection.ensure_connected()
        declare_topology(channel, manager_queue_alpha)
        declare_topology(channel, manager_queue_beta)

        print(f"Exchange: {EXCHANGE_NAME}")
        print(f"Broadcast routing key: {BROADCAST_ROUTING_KEY}")
        logger.info(
            "Topology declared for two managers",
            manager_alpha=manager_queue_alpha,
            manager_beta=manager_queue_beta,
        )

        # --- Start both consumers ---
        alpha_received_signal = threading.Event()
        beta_received_signal = threading.Event()

        def handle_alpha_command(message: CommandMessage) -> None:
            logger.info(
                "Manager alpha received broadcast",
                command=message.command.name,
                app_id=message.app_id[:16] + "...",
            )
            alpha_received_signal.set()

        def handle_beta_command(message: CommandMessage) -> None:
            logger.info(
                "Manager beta received broadcast",
                command=message.command.name,
                app_id=message.app_id[:16] + "...",
            )
            beta_received_signal.set()

        alpha_settings = RabbitMQSettings(queue=manager_queue_alpha)
        beta_settings = RabbitMQSettings(queue=manager_queue_beta)

        alpha_consumer = CommandConsumer(alpha_connection, alpha_settings, handle_alpha_command)
        beta_consumer = CommandConsumer(beta_connection, beta_settings, handle_beta_command)

        alpha_consumer.start()
        beta_consumer.start()
        time.sleep(0.5)
        logger.info("Both consumers running")

        # --- Serialization round-trip ---
        print("\n--- Serialization round-trip ---")
        command_message = CommandMessage(
            app_id="b" * 64,
            command=CmdName.STOP,
            issued_by="scheduler@system",
        )
        json_bytes = command_message.to_json_bytes()
        print(f"JSON bytes: {json_bytes}")

        reconstructed = CommandMessage.from_json_bytes(json_bytes)
        print(f"Reconstructed: command={reconstructed.command.name}, app_id={reconstructed.app_id[:16]}...")
        logger.info(
            "Serialization round-trip verified",
            original_command=command_message.command.name,
            reconstructed_command=reconstructed.command.name,
        )

        # --- Broadcast ---
        print("\n--- Broadcasting STOP command ---")
        publisher = CommandPublisher(publisher_connection)
        publisher.broadcast(command_message)
        logger.info("Broadcast sent", routing_key=BROADCAST_ROUTING_KEY)

        # --- Wait for both managers ---
        alpha_delivered = alpha_received_signal.wait(timeout=5.0)
        beta_delivered = beta_received_signal.wait(timeout=5.0)

        print()
        if alpha_delivered and beta_delivered:
            logger.info("Both managers received the broadcast")
        else:
            logger.warning(
                "Not all managers received the broadcast",
                alpha=alpha_delivered,
                beta=beta_delivered,
            )

    finally:
        print("\n--- Cleanup ---")
        if alpha_consumer is not None:
            alpha_consumer.stop()
        if beta_consumer is not None:
            beta_consumer.stop()
        publisher_connection.close()
        alpha_connection.close()
        beta_connection.close()
        logging_service.stop()
        print("Cleanup complete.")


if __name__ == "__main__":
    main()
