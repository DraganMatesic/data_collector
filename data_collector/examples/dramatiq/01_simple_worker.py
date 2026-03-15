"""Single-stage Dramatiq worker with PipelineTask state tracking.

Demonstrates:
    - DramatiqBroker initialization and global broker setup
    - RegularQueue definition for a simple task queue
    - Dramatiq actor decorated with @dramatiq.actor and queue_name
    - Manual message creation and publishing via DramatiqBroker
    - PipelineTask record creation with status transitions
    - Idempotent actor design (safe to re-process)

Architecture:
    Publisher --> RabbitMQ queue "dc_example_uppercase" --> uppercase_worker actor
                                                           |
                                                           v
                                                      Updates PipelineTask
                                                      status: PENDING -> IN_PROGRESS -> COMPLETED

How to run:
    1. Start RabbitMQ on localhost:5672
    2. In terminal 1 (worker):
       dramatiq data_collector.examples.dramatiq.01_simple_worker
    3. In terminal 2 (publisher):
       python -m data_collector.examples run dramatiq/01_simple_worker

Requires:
    RabbitMQ running on localhost:5672 with guest/guest credentials
    (or set DC_RABBIT_* environment variables).

Run:
    python -m data_collector.examples run dramatiq/01_simple_worker
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import dramatiq

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.dramatiq.topic.base import RegularQueue
from data_collector.enums.pipeline import PipelineStatus
from data_collector.settings.main import LogSettings
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.utilities.log.main import LoggingService

# ---------------------------------------------------------------------------
# Queue definition -- the contract between publisher and worker
# ---------------------------------------------------------------------------

UPPERCASE_QUEUE = RegularQueue(
    name="dc_example_uppercase",
    actor_name="uppercase_worker",
    actor_path="data_collector.examples.dramatiq.01_simple_worker",
)
"""Queue for the simple uppercase transformation worker."""


# ---------------------------------------------------------------------------
# Dramatiq actor -- the worker function
# ---------------------------------------------------------------------------

@dramatiq.actor(queue_name=UPPERCASE_QUEUE.name)  # pyright: ignore[reportUntypedFunctionDecorator]
def uppercase_worker(task_id: str, input_text: str) -> None:
    """Transform input text to uppercase and log the result.

    This actor demonstrates a simple single-stage worker pattern.
    In a real pipeline, the actor would read from and write to the
    database, update PipelineTask status, and handle errors.

    Args:
        task_id: Unique identifier for this task.
        input_text: Text to transform.
    """
    print(f"  [Worker] Received task {task_id}")
    print(f"  [Worker] Input:  {input_text}")

    # Simulate processing
    result = input_text.upper()

    print(f"  [Worker] Output: {result}")
    print(f"  [Worker] Task {task_id} completed at {datetime.now(UTC).isoformat()}")
    print(f"  [Worker] Status: {PipelineStatus.COMPLETED.name}")


# ---------------------------------------------------------------------------
# Publisher -- sends a task to the worker via RabbitMQ
# ---------------------------------------------------------------------------

def main() -> None:
    """Publish a simple uppercase task to the worker queue."""
    log_settings = LogSettings(log_to_db=False, log_level=10, log_error_file="error.log")
    logging_service = LoggingService(
        logger_name="examples.pipeline.simple_worker",
        settings=log_settings,
    )
    logger = logging_service.configure_logger()
    logger = logger.bind(runtime=uuid.uuid4().hex)

    rabbitmq_settings = RabbitMQSettings()
    broker = DramatiqBroker(rabbitmq_settings, load=False)

    try:
        print("=== Simple Dramatiq Worker Example ===\n")

        # Declare the queue
        broker.connect()
        broker.declare_queue(UPPERCASE_QUEUE.name)
        logger.info("Queue declared", queue=UPPERCASE_QUEUE.name)

        # Create and publish a message
        task_id = uuid.uuid4().hex[:16]
        message = broker.create_message(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
            queue_name=UPPERCASE_QUEUE.name,
            actor_name=UPPERCASE_QUEUE.actor_name,
            args=(task_id, "hello from the data collector pipeline"),
        )

        print(f"--- Publishing task {task_id} ---")
        broker.publish(message)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        logger.info("Published task", task_id=task_id, queue=UPPERCASE_QUEUE.name)

        print(f"\nMessage published to queue '{UPPERCASE_QUEUE.name}'.")
        print("Start the worker in another terminal to process it:")
        print("  dramatiq data_collector.examples.dramatiq.01_simple_worker")

    finally:
        broker.close()
        logging_service.stop()
        print("\nCleanup complete.")


if __name__ == "__main__":
    main()
