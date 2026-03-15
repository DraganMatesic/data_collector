"""Multi-stage Dramatiq pipeline with decoupled workers.

Demonstrates:
    - TopicExchange with routing keys for multi-stage pipeline
    - Two workers that do NOT import each other
    - Stage 1 publishes to stage 2 via routing key (not direct import)
    - Shared topic module as the contract between stages
    - Worker isolation: each worker knows only its own task and the
      routing key for the next stage

Architecture:
    Publisher --> TopicExchange "dc_example_topic"
                     |
                     +-- routing_key "example.new.data"
                     |       --> Queue "dc_example_prepare"
                     |               --> prepare_data actor (Stage 1)
                     |                       |
                     |                 publishes to routing_key
                     |                 "example.prepared.data"
                     |                       |
                     +-- routing_key "example.prepared.data"
                             --> Queue "dc_example_process"
                                     --> process_data actor (Stage 2)
                                             |
                                          Done

Key insight: prepare_data does NOT import process_data.  It publishes
to PROCESS_QUEUE.routing_key.  The topic exchange routes the message
to the correct queue.  Neither worker knows the other exists.

How to run:
    1. Start RabbitMQ on localhost:5672
    2. In terminal 1 (workers):
       dramatiq data_collector.examples.dramatiq.02_multi_stage_pipeline
    3. In terminal 2 (publisher):
       python -m data_collector.examples run dramatiq/02_multi_stage_pipeline

Requires:
    RabbitMQ running on localhost:5672 with guest/guest credentials
    (or set DC_RABBIT_* environment variables).

Run:
    python -m data_collector.examples run dramatiq/02_multi_stage_pipeline
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import dramatiq
from pika.exchange_type import ExchangeType

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.dramatiq.topic.base import TopicExchange, TopicExchangeQueue
from data_collector.settings.main import LogSettings
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.utilities.log.main import LoggingService

# ---------------------------------------------------------------------------
# Shared topic module -- the CONTRACT between stages
# Both workers import this module, but never import each other.
# ---------------------------------------------------------------------------

EXAMPLE_TOPIC_EXCHANGE = TopicExchange(
    name="dc_example_topic",
    exchange_type=ExchangeType.topic,
)
"""Topic exchange for the example multi-stage pipeline."""

PREPARE_QUEUE = TopicExchangeQueue(
    name="dc_example_prepare",
    actor_name="prepare_data",
    exchange_name=EXAMPLE_TOPIC_EXCHANGE.name,
    routing_key="example.new.data",
    actor_path="data_collector.examples.dramatiq.02_multi_stage_pipeline",
)
"""Stage 1: Prepare raw data for processing."""

PROCESS_QUEUE = TopicExchangeQueue(
    name="dc_example_process",
    actor_name="process_data",
    exchange_name=EXAMPLE_TOPIC_EXCHANGE.name,
    routing_key="example.prepared.data",
    actor_path="data_collector.examples.dramatiq.02_multi_stage_pipeline",
)
"""Stage 2: Process prepared data into final output."""


# ---------------------------------------------------------------------------
# Stage 1: Prepare data
# This worker does NOT import process_data.  It publishes to the
# PROCESS_QUEUE routing key.  The topic exchange handles routing.
# ---------------------------------------------------------------------------

@dramatiq.actor(queue_name=PREPARE_QUEUE.name)  # pyright: ignore[reportUntypedFunctionDecorator]
def prepare_data(task_id: str, raw_data: str) -> None:
    """Stage 1: Clean and normalize raw data, then forward to stage 2.

    Args:
        task_id: Unique identifier for this task.
        raw_data: Raw input data to prepare.
    """
    print(f"  [Stage 1 - Prepare] Task {task_id}")
    print(f"  [Stage 1 - Prepare] Raw input: {raw_data}")

    # Simulate data preparation: strip whitespace, normalize
    prepared = raw_data.strip().lower().replace("  ", " ")
    print(f"  [Stage 1 - Prepare] Prepared: {prepared}")

    # Publish to Stage 2 via routing key -- NOT by importing process_data
    rabbitmq_settings = RabbitMQSettings()
    broker = DramatiqBroker(rabbitmq_settings, load=False)
    broker.connect()

    message = broker.create_message(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
        queue_name=PROCESS_QUEUE.name,
        actor_name=PROCESS_QUEUE.actor_name,
        args=(task_id, prepared),
    )
    broker.publish(  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        message,
        exchange_name=PROCESS_QUEUE.exchange_name,
        routing_key=PROCESS_QUEUE.routing_key,
    )
    broker.close()

    print(f"  [Stage 1 - Prepare] Forwarded to stage 2 via routing_key='{PROCESS_QUEUE.routing_key}'")


# ---------------------------------------------------------------------------
# Stage 2: Process data
# This worker does NOT know about prepare_data.  It receives messages
# routed by the topic exchange.
# ---------------------------------------------------------------------------

@dramatiq.actor(queue_name=PROCESS_QUEUE.name)  # pyright: ignore[reportUntypedFunctionDecorator]
def process_data(task_id: str, prepared_data: str) -> None:
    """Stage 2: Process prepared data into final structured output.

    Args:
        task_id: Unique identifier for this task.
        prepared_data: Cleaned data from stage 1.
    """
    print(f"  [Stage 2 - Process] Task {task_id}")
    print(f"  [Stage 2 - Process] Input: {prepared_data}")

    # Simulate processing: word count, character analysis
    words = prepared_data.split()
    result = {
        "word_count": len(words),
        "char_count": len(prepared_data),
        "unique_words": len(set(words)),
        "processed_at": datetime.now(UTC).isoformat(),
    }

    print(f"  [Stage 2 - Process] Result: {result}")
    print(f"  [Stage 2 - Process] Task {task_id} COMPLETED")


# ---------------------------------------------------------------------------
# Publisher -- injects an event into Stage 1 of the pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    """Publish an event to start the multi-stage pipeline."""
    log_settings = LogSettings(log_to_db=False, log_level=10, log_error_file="error.log")
    logging_service = LoggingService(
        logger_name="examples.pipeline.multi_stage",
        settings=log_settings,
    )
    logger = logging_service.configure_logger()
    logger = logger.bind(runtime=uuid.uuid4().hex)

    rabbitmq_settings = RabbitMQSettings()
    broker = DramatiqBroker(rabbitmq_settings, load=False)

    try:
        print("=== Multi-Stage Pipeline Example ===\n")

        # Declare topology
        broker.connect()
        broker.declare_exchange_and_bind(EXAMPLE_TOPIC_EXCHANGE, [PREPARE_QUEUE, PROCESS_QUEUE])
        logger.info("Topology declared", exchange=EXAMPLE_TOPIC_EXCHANGE.name)

        # Publish to Stage 1
        task_id = uuid.uuid4().hex[:16]
        raw_data = "  Hello World   from the   Data Collector   Pipeline  "

        message = broker.create_message(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
            queue_name=PREPARE_QUEUE.name,
            actor_name=PREPARE_QUEUE.actor_name,
            args=(task_id, raw_data),
        )

        print(f"--- Publishing task {task_id} to Stage 1 ---")
        print(f"    Raw data: '{raw_data}'")
        broker.publish(  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            message,
            exchange_name=PREPARE_QUEUE.exchange_name,
            routing_key=PREPARE_QUEUE.routing_key,
        )
        logger.info("Published to stage 1", task_id=task_id)

        print(f"\nMessage published to exchange '{EXAMPLE_TOPIC_EXCHANGE.name}'")
        print(f"  routing_key='{PREPARE_QUEUE.routing_key}'")
        print("\nThe message will flow: Stage 1 (prepare) --> Stage 2 (process)")
        print("\nStart the workers in another terminal:")
        print("  dramatiq data_collector.examples.dramatiq.02_multi_stage_pipeline")

    finally:
        broker.close()
        logging_service.stop()
        print("\nCleanup complete.")


if __name__ == "__main__":
    main()
