"""Topology dataclasses and pre-defined exchanges/queues for Dramatiq pipelines.

Queue topology is defined declaratively via frozen dataclasses.  This module
provides a single source of truth for all exchanges, queues, and routing
keys -- code drives RabbitMQ state, not the other way around.

Country or domain-specific apps define their queues in a ``topics.py`` file
inside their app namespace.  The ``get_topic_modules()`` function discovers
these files automatically at startup -- no manual registration required.
"""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass, field
from pathlib import Path

from pika.exchange_type import ExchangeType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TopicExchange:
    """RabbitMQ topic exchange definition.

    Args:
        name: Exchange name in RabbitMQ.
        durable: Whether the exchange survives broker restarts.
        exchange_type: RabbitMQ exchange type (topic, fanout, direct).
        arguments: Additional exchange arguments (e.g. alternate-exchange).
    """

    name: str
    durable: bool = True
    exchange_type: ExchangeType = ExchangeType.topic
    arguments: dict[str, str] = field(default_factory=lambda: {})  # pyright: ignore[reportUnknownVariableType]


@dataclass(frozen=True)
class TopicExchangeQueue:
    """Queue bound to a topic exchange with a routing key.

    Args:
        name: Queue name in RabbitMQ.
        actor_name: Dramatiq actor function name that consumes this queue.
        durable: Whether the queue survives broker restarts.
        exchange_name: Name of the exchange to bind to.
        routing_key: Topic routing key pattern for message routing.
        actor_path: Python module path for AST-based actor validation.
    """

    name: str
    actor_name: str
    durable: bool = True
    exchange_name: str = ""
    routing_key: str = ""
    actor_path: str = ""


@dataclass(frozen=True)
class RegularQueue:
    """Standard queue without exchange routing.

    Args:
        name: Queue name in RabbitMQ.
        durable: Whether the queue survives broker restarts.
        actor_name: Dramatiq actor function name that consumes this queue.
        actor_path: Python module path for AST-based actor validation.
    """

    name: str
    durable: bool = True
    actor_name: str = ""
    actor_path: str = ""


# ---------------------------------------------------------------------------
# Pre-defined exchanges
# ---------------------------------------------------------------------------

UNROUTABLE_EXCHANGE = TopicExchange(
    name="dc_unroutable",
    exchange_type=ExchangeType.fanout,
)
"""Fanout exchange for messages with no matching routing key.

Used as the ``alternate-exchange`` on topic exchanges to ensure no message
is silently lost -- unroutable messages are forwarded here for inspection.
"""

OCR_TOPIC_EXCHANGE = TopicExchange(
    name="dc_ocr_topic",
    exchange_type=ExchangeType.topic,
    arguments={"alternate-exchange": UNROUTABLE_EXCHANGE.name},
)
"""Topic exchange for OCR/document processing pipelines.

Routing key pattern: ``ocr.{stage}.{document_type}``
"""

# ---------------------------------------------------------------------------
# Pre-defined queues
# ---------------------------------------------------------------------------

DEAD_LETTERS_QUEUE = RegularQueue(
    name="dc_dead_letters",
    actor_name="log_dead_letter",
    actor_path="data_collector.dramatiq.workers.dead_letters",
)
"""Queue for messages that exhausted all retry attempts."""

LOGGING_TEST_QUEUE = RegularQueue(
    name="dc_logging_test",
    actor_name="logging_test_worker",
    actor_path="data_collector.dramatiq.workers.logging_test",
)
"""Test queue for verifying logging and @fun_watch in Dramatiq workers. Remove after verification."""

# ---------------------------------------------------------------------------
# Convention-based topic module discovery
# ---------------------------------------------------------------------------

_CORE_TOPIC_MODULE: str = "data_collector.dramatiq.topic.base"
"""Module path for the core topology definitions (always loaded first)."""

_FRAMEWORK_DIRECTORIES: frozenset[str] = frozenset({
    "captcha",
    "dramatiq",
    "enums",
    "messaging",
    "notifications",
    "orchestration",
    "proxy",
    "scaffold",
    "scraping",
    "settings",
    "tables",
    "utilities",
})
"""Top-level directories under ``data_collector/`` that are framework internals, not app namespaces."""


def _package_root() -> Path:
    """Return the ``data_collector/`` package root directory."""
    return Path(__file__).resolve().parent.parent.parent


@functools.lru_cache(maxsize=1)
def get_topic_modules() -> list[str]:
    """Discover topic modules by scanning app namespaces for ``topics.py`` files.

    Walks the ``data_collector/`` package directory tree, finds ``topics.py``
    files in app namespaces (excluding framework directories), and converts
    filesystem paths to Python module paths.

    The core topic module (``data_collector.dramatiq.topic.base``) is always
    first in the returned list.  Discovered modules are appended in sorted
    order for deterministic behavior.

    Returns:
        List of Python module path strings.
    """
    package_root = _package_root()
    discovered: list[str] = []

    for topics_path in package_root.rglob("topics.py"):
        relative = topics_path.relative_to(package_root)
        parts = relative.parts

        if len(parts) < 2:
            continue

        if parts[0] in _FRAMEWORK_DIRECTORIES:
            continue

        if "__pycache__" in parts:
            continue

        module_path = "data_collector." + ".".join(relative.with_suffix("").parts)
        discovered.append(module_path)

    discovered.sort()
    logger.debug("Discovered %d app topic module(s): %s", len(discovered), discovered)
    return [_CORE_TOPIC_MODULE, *discovered]
