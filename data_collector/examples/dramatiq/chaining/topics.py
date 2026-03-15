"""Dramatiq topic and queue definitions for examples.dramatiq.chaining.

Three-stage pipeline: prepare -> process -> validate.
Each stage publishes to the next via routing key on a shared topic exchange.

The framework auto-discovers this file at startup via ``get_topic_modules()``.
No manual registration required.
"""

from data_collector.dramatiq.topic.base import TopicExchange, TopicExchangeQueue

CHAINING_EXCHANGE = TopicExchange(
    name="dc_chaining_topic",
)
"""Topic exchange for the chaining pipeline example."""

MAIN_EXCHANGE_QUEUE = TopicExchangeQueue(
    name="dc_chaining_prepare",
    actor_name="chaining_prepare",
    exchange_name=CHAINING_EXCHANGE.name,
    routing_key="chaining.new.data",
    actor_path="data_collector.examples.dramatiq.chaining.main",
)
"""Stage 1: Prepare. TaskDispatcher reads this constant to dispatch events."""

PROCESS_QUEUE = TopicExchangeQueue(
    name="dc_chaining_process",
    actor_name="chaining_process",
    exchange_name=CHAINING_EXCHANGE.name,
    routing_key="chaining.prepared.data",
    actor_path="data_collector.examples.dramatiq.chaining.main",
)
"""Stage 2: Process."""

VALIDATE_QUEUE = TopicExchangeQueue(
    name="dc_chaining_validate",
    actor_name="chaining_validate",
    exchange_name=CHAINING_EXCHANGE.name,
    routing_key="chaining.processed.data",
    actor_path="data_collector.examples.dramatiq.chaining.main",
)
"""Stage 3: Validate (final)."""
