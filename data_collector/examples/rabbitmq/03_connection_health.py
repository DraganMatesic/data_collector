"""Connection lifecycle, health checks, and auto-reconnect demonstration.

Demonstrates:
    - RabbitMQSettings field display (configuration summary)
    - RabbitMQConnection.is_healthy() across connect/close/reconnect states
    - Connection lifecycle: connect, verify, close, verify again
    - ensure_connected() triggering automatic reconnection
    - Topology declaration idempotency (safe to call multiple times)
    - CommandPublisher.publish() with explicit routing key
    - Publishing after auto-reconnect confirms channel recovery

Requires:
    RabbitMQ running on localhost:5672 with guest/guest credentials
    (or set DC_RABBIT_* environment variables).

Run:
    python -m data_collector.examples run rabbitmq/03_connection_health
"""

from __future__ import annotations

from data_collector.enums.commands import CmdName
from data_collector.messaging import (
    CommandMessage,
    CommandPublisher,
    RabbitMQConnection,
    declare_topology,
)
from data_collector.settings.rabbitmq import RabbitMQSettings


def main() -> None:
    """Walk through connection states and verify health at each step."""
    settings = RabbitMQSettings()

    print("=== RabbitMQ Connection Health Example ===\n")
    print("--- Configuration ---")
    print(f"  Host: {settings.host}:{settings.port}")
    print(f"  Queue: {settings.queue}")
    print(f"  Prefetch: {settings.prefetch}")
    print(f"  Heartbeat: {settings.heartbeat}s")
    print(f"  Reconnect: max {settings.reconnect_max_attempts} attempts, "
          f"base delay {settings.reconnect_base_delay}s, "
          f"max delay {settings.reconnect_max_delay}s")

    connection = RabbitMQConnection(settings)

    try:
        # Step 1: Health before connect
        print("\n--- Step 1: Health before connect ---")
        print(f"  Healthy: {connection.is_healthy()}")

        # Step 2: Connect and verify
        print("\n--- Step 2: Connect ---")
        connection.connect()
        print(f"  Healthy: {connection.is_healthy()}")

        # Step 3: Topology idempotency
        print("\n--- Step 3: Topology idempotency ---")
        channel = connection.ensure_connected()
        declare_topology(channel, settings.queue)
        print("  Topology declared (first call)")
        declare_topology(channel, settings.queue)
        print("  Topology declared (second call -- idempotent, no error)")

        # Step 4: Publish with explicit routing key
        print("\n--- Step 4: Publish with explicit routing key ---")
        enable_message = CommandMessage(
            app_id="c" * 64,
            command=CmdName.ENABLE,
            issued_by="admin@example.com",
        )
        publisher = CommandPublisher(connection)
        publisher.publish(enable_message, routing_key=settings.queue)
        print(f"  Published {enable_message.command.name} to routing_key='{settings.queue}'")

        # Step 5: Close and verify
        print("\n--- Step 5: Close connection ---")
        connection.close()
        print(f"  Healthy: {connection.is_healthy()}")

        # Step 6: Auto-reconnect via ensure_connected()
        print("\n--- Step 6: Auto-reconnect via ensure_connected() ---")
        connection.ensure_connected()
        print(f"  Healthy: {connection.is_healthy()} (reconnected automatically)")

        # Step 7: Publish after reconnect
        print("\n--- Step 7: Publish after reconnect ---")
        disable_message = CommandMessage(
            app_id="d" * 64,
            command=CmdName.DISABLE,
            issued_by="admin@example.com",
        )
        publisher.publish(disable_message, routing_key=settings.queue)
        print(f"  Published {disable_message.command.name} to routing_key='{settings.queue}'")

        print("\nAll health check scenarios passed.")

    finally:
        connection.close()
        print("Connection closed. Cleanup complete.")


if __name__ == "__main__":
    main()
