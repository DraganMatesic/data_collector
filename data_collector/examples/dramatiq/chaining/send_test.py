"""Send a test message to the chaining pipeline.

Usage::

    python -m data_collector.examples.dramatiq.chaining.send_test
    python -m data_collector.examples.dramatiq.chaining.send_test "my_payload"
"""

from __future__ import annotations

import sys

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.settings.rabbitmq import RabbitMQSettings

from .topics import MAIN_EXCHANGE_QUEUE


def main() -> None:
    """Publish a single message to stage 1 of the chaining pipeline."""
    payload = sys.argv[1] if len(sys.argv) > 1 else "hello_pipeline"

    broker = DramatiqBroker(RabbitMQSettings(), load=False)
    broker.connect()
    message = broker.create_message(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
        queue_name=MAIN_EXCHANGE_QUEUE.name,
        actor_name=MAIN_EXCHANGE_QUEUE.actor_name,
        args=(payload,),
    )
    broker.publish(  # pyright: ignore[reportUnknownMemberType]
        message,
        exchange_name=MAIN_EXCHANGE_QUEUE.exchange_name,
        routing_key=MAIN_EXCHANGE_QUEUE.routing_key,
    )
    broker.close()
    print(f"Sent '{payload}' to {MAIN_EXCHANGE_QUEUE.name}")


if __name__ == "__main__":
    main()
