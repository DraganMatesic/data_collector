"""Dramatiq broker management with RabbitMQ topology lifecycle.

The ``DramatiqBroker`` manages the full broker lifecycle -- initialization,
exchange/queue declaration, binding synchronization, AST-based actor
validation, and topology self-healing.  It uses a separate
``RabbitMQConnection`` for topology management (exchange declaration, binding
sync) while Dramatiq manages its own consumer connections internally.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import logging
from collections import defaultdict
from typing import Any

import dramatiq
import dramatiq.brokers.rabbitmq
import httpx
import pika
from dramatiq.middleware import AgeLimit, Retries, TimeLimit

from data_collector.dramatiq.retry import should_retry_message
from data_collector.dramatiq.topic.base import (
    RegularQueue,
    TopicExchange,
    TopicExchangeQueue,
    get_topic_modules,
)
from data_collector.messaging.connection import RabbitMQConnection
from data_collector.settings.dramatiq import DramatiqSettings
from data_collector.settings.rabbitmq import RabbitMQSettings

logger = logging.getLogger(__name__)

_MANAGED_PREFIX: str = "dc_"
"""Only queues with this prefix are managed by topology self-healing."""


class DramatiqBroker:
    """Manages Dramatiq broker lifecycle and RabbitMQ topology.

    Creates a Dramatiq ``RabbitmqBroker``, sets it as the global broker,
    and optionally declares all exchanges, synchronizes bindings, and
    removes orphan queues on startup (when ``load=True``).

    Args:
        rabbitmq_settings: Connection parameters for RabbitMQ.
        dramatiq_settings: Worker and retry configuration.  Uses defaults
            if not provided.
        load: When True, declare exchanges, sync bindings, and clean
            dead queues on construction.
    """

    def __init__(
        self,
        rabbitmq_settings: RabbitMQSettings,
        dramatiq_settings: DramatiqSettings | None = None,
        load: bool = False,
    ) -> None:
        self._rabbitmq_settings = rabbitmq_settings
        self._dramatiq_settings = dramatiq_settings or DramatiqSettings()
        self._broker = self._create_broker()
        dramatiq.set_broker(self._broker)

        self._management_connection = RabbitMQConnection(rabbitmq_settings)

        if load:
            self._management_connection.connect()
            self._declare_exchanges()
            self._sync_bindings()
            self._clean_dead_queues()

    @property
    def broker(self) -> dramatiq.brokers.rabbitmq.RabbitmqBroker:
        """Return the underlying Dramatiq broker instance."""
        return self._broker

    @property
    def dramatiq_settings(self) -> DramatiqSettings:
        """Return the Dramatiq settings."""
        return self._dramatiq_settings

    # ------------------------------------------------------------------
    # Broker creation
    # ------------------------------------------------------------------

    def _create_broker(self) -> dramatiq.brokers.rabbitmq.RabbitmqBroker:
        """Create a Dramatiq RabbitmqBroker from settings.

        Dramatiq's ``RabbitmqBroker`` expects ``parameters`` as a list of
        dicts (which it passes to ``pika.ConnectionParameters(**dict)``),
        not pre-built ``ConnectionParameters`` objects.
        """
        connection_params: dict[str, object] = {
            "host": self._rabbitmq_settings.host,
            "port": self._rabbitmq_settings.port,
            "credentials": pika.PlainCredentials(
                self._rabbitmq_settings.username,
                self._rabbitmq_settings.password,
            ),
            "heartbeat": self._rabbitmq_settings.heartbeat,
        }
        broker = dramatiq.brokers.rabbitmq.RabbitmqBroker(  # pyright: ignore[reportUnknownMemberType, reportCallIssue]
            parameters=[connection_params],
        )
        self._configure_middleware(broker)
        logger.info(
            "Created Dramatiq broker for %s:%d",
            self._rabbitmq_settings.host,
            self._rabbitmq_settings.port,
        )
        return broker

    def _configure_middleware(self, broker: dramatiq.brokers.rabbitmq.RabbitmqBroker) -> None:
        """Replace default middleware with framework-configured versions.

        Dramatiq creates its default middleware stack (``AgeLimit``,
        ``TimeLimit``, ``ShutdownNotifications``, ``Callbacks``,
        ``Pipelines``, ``Retries``) with library defaults. This method
        replaces ``TimeLimit``, ``AgeLimit``, and ``Retries`` with
        instances configured from ``DramatiqSettings``.
        """
        new_middleware: list[dramatiq.Middleware] = []
        for middleware in broker.middleware:
            if isinstance(middleware, TimeLimit):
                new_middleware.append(
                    TimeLimit(time_limit=self._dramatiq_settings.time_limit)  # pyright: ignore[reportCallIssue]
                )
            elif isinstance(middleware, AgeLimit):
                new_middleware.append(
                    AgeLimit(max_age=self._dramatiq_settings.max_age)  # pyright: ignore[reportCallIssue]
                )
            elif isinstance(middleware, Retries):
                new_middleware.append(
                    Retries(  # pyright: ignore[reportCallIssue]
                        max_retries=self._dramatiq_settings.max_retries,
                        min_backoff=self._dramatiq_settings.min_backoff,
                        max_backoff=self._dramatiq_settings.max_backoff,
                        retry_when=should_retry_message,
                    )
                )
            else:
                new_middleware.append(middleware)
        broker.middleware = new_middleware

    # ------------------------------------------------------------------
    # Topology discovery
    # ------------------------------------------------------------------

    def _get_all_exchanges(self) -> list[TopicExchange]:
        """Collect all TopicExchange instances from registered topic modules."""
        exchanges: list[TopicExchange] = []
        for module_path in get_topic_modules():
            try:
                module = importlib.import_module(module_path)
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if isinstance(attribute, TopicExchange):
                        exchanges.append(attribute)
            except ImportError:
                logger.warning("Could not import topic module: %s", module_path)
        return exchanges

    def _get_all_queues(self) -> list[TopicExchangeQueue | RegularQueue]:
        """Collect all queue instances from registered topic modules."""
        queues: list[TopicExchangeQueue | RegularQueue] = []
        for module_path in get_topic_modules():
            try:
                module = importlib.import_module(module_path)
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if isinstance(attribute, (TopicExchangeQueue, RegularQueue)):
                        queues.append(attribute)
            except ImportError:
                logger.warning("Could not import topic module: %s", module_path)
        return queues

    # ------------------------------------------------------------------
    # Exchange and queue declaration
    # ------------------------------------------------------------------

    def _declare_exchanges(self) -> None:
        """Declare all TopicExchange instances on RabbitMQ."""
        channel = self._management_connection.ensure_connected()
        exchanges = self._get_all_exchanges()

        for exchange in exchanges:
            channel.exchange_declare(  # pyright: ignore[reportUnknownMemberType]
                exchange=exchange.name,
                exchange_type=exchange.exchange_type.value,
                durable=exchange.durable,
                arguments=exchange.arguments if exchange.arguments else None,
            )
            logger.debug("Declared exchange '%s' (%s)", exchange.name, exchange.exchange_type.value)

    # ------------------------------------------------------------------
    # AST-based actor validation
    # ------------------------------------------------------------------

    def _actor_exists(self, actor_path: str, actor_name: str) -> bool:
        """Validate that an actor function exists via AST inspection.

        Checks the target module's source code for a function definition
        matching ``actor_name`` without importing the module.  This avoids
        circular imports and side effects during topology validation.

        Args:
            actor_path: Python module path (e.g. ``data_collector.dramatiq.workers.dead_letters``).
            actor_name: Function name to look for.

        Returns:
            True if the function exists in the module source.
        """
        spec = importlib.util.find_spec(actor_path)
        if spec is None or spec.origin is None:
            logger.warning("Module not found for actor validation: %s", actor_path)
            return False

        try:
            with open(spec.origin, encoding="utf-8") as source_file:
                tree = ast.parse(source_file.read())
        except (OSError, SyntaxError):
            logger.warning("Could not parse module for actor validation: %s", actor_path, exc_info=True)
            return False

        return any(isinstance(node, ast.FunctionDef) and node.name == actor_name for node in ast.walk(tree))

    def _ensure_queue_declared(self, queue_name: str) -> None:
        """Declare a queue if it does not already exist.

        Creates the queue with Dramatiq-compatible arguments
        (``x-dead-letter-exchange``, ``x-dead-letter-routing-key``)
        so that Dramatiq's retry mechanism works correctly.
        """
        channel = self._management_connection.ensure_connected()
        try:
            channel.queue_declare(queue=queue_name, passive=True)  # pyright: ignore[reportUnknownMemberType]
        except pika.exceptions.ChannelClosedByBroker:  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
            channel = self._management_connection.ensure_connected()
            channel.queue_declare(  # pyright: ignore[reportUnknownMemberType]
                queue=queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": f"{queue_name}.XQ",
                },
            )
            logger.debug("Declared queue '%s'", queue_name)

    # ------------------------------------------------------------------
    # Binding synchronization
    # ------------------------------------------------------------------

    def _sync_bindings(self) -> None:
        """Synchronize RabbitMQ bindings to match code-defined topology.

        For each exchange, compares the desired bindings (from code) against
        the existing bindings (from the Management API).  Removes stale
        bindings and creates missing ones.  Only binds queues whose actors
        pass AST validation.
        """
        queues = self._get_all_queues()
        by_exchange: dict[str, list[TopicExchangeQueue]] = defaultdict(list)

        for queue_definition in queues:
            if isinstance(queue_definition, TopicExchangeQueue) and queue_definition.exchange_name:
                by_exchange[queue_definition.exchange_name].append(queue_definition)

        channel = self._management_connection.ensure_connected()

        for exchange_name, exchange_queues in by_exchange.items():
            existing_bindings = self._get_exchange_bindings(exchange_name)
            desired_bindings = {(queue.name, queue.routing_key) for queue in exchange_queues}

            # Remove stale bindings
            for binding in existing_bindings:
                binding_key = (binding["destination"], binding["routing_key"])
                if binding_key not in desired_bindings:
                    channel.queue_unbind(  # pyright: ignore[reportUnknownMemberType]
                        queue=binding["destination"],
                        exchange=exchange_name,
                        routing_key=binding["routing_key"],
                    )
                    logger.info(
                        "Removed stale binding: %s -> %s (key=%s)",
                        exchange_name,
                        binding["destination"],
                        binding["routing_key"],
                    )

            # Create missing bindings
            existing_binding_keys = {(binding["destination"], binding["routing_key"]) for binding in existing_bindings}
            for queue_definition in exchange_queues:
                if (queue_definition.name, queue_definition.routing_key) not in existing_binding_keys:
                    if queue_definition.actor_path and not self._actor_exists(
                        queue_definition.actor_path, queue_definition.actor_name
                    ):
                        logger.warning(
                            "Skipping binding for '%s': actor '%s' not found in '%s'",
                            queue_definition.name,
                            queue_definition.actor_name,
                            queue_definition.actor_path,
                        )
                        continue
                    self._ensure_queue_declared(queue_definition.name)
                    channel = self._management_connection.ensure_connected()
                    channel.queue_bind(  # pyright: ignore[reportUnknownMemberType]
                        queue=queue_definition.name,
                        exchange=exchange_name,
                        routing_key=queue_definition.routing_key,
                    )
                    logger.info(
                        "Created binding: %s -> %s (key=%s)",
                        exchange_name,
                        queue_definition.name,
                        queue_definition.routing_key,
                    )

    # ------------------------------------------------------------------
    # Topology self-healing
    # ------------------------------------------------------------------

    def _clean_dead_queues(self) -> None:
        """Remove queues from RabbitMQ that no longer exist in code.

        Only queues with the ``dc_`` prefix are managed.  Dramatiq's
        internal ``.DQ`` (delay) and ``.XQ`` (dead-letter) queues are
        preserved.  This prevents accidental deletion of queues belonging
        to other applications.
        """
        active_queue_names = {queue.name for queue in self._get_all_queues()}
        # Also preserve Dramatiq's internal queues (*.DQ delay, *.XQ dead-letter)
        active_with_internal = active_queue_names | {
            f"{name}.DQ" for name in active_queue_names
        } | {
            f"{name}.XQ" for name in active_queue_names
        }
        broker_queue_names = self._get_broker_queues()

        for queue_name in broker_queue_names:
            if queue_name.startswith(_MANAGED_PREFIX) and queue_name not in active_with_internal:
                channel = self._management_connection.ensure_connected()
                channel.queue_delete(queue=queue_name)  # pyright: ignore[reportUnknownMemberType]
                logger.warning("Removed orphan queue: %s", queue_name)

    # ------------------------------------------------------------------
    # Management API helpers
    # ------------------------------------------------------------------

    def _management_api_url(self, path: str) -> str:
        """Build a RabbitMQ Management API URL."""
        return (
            f"http://{self._rabbitmq_settings.host}"
            f":{self._rabbitmq_settings.management_port}"
            f"/api{path}"
        )

    def _management_api_get(self, path: str) -> Any:
        """Execute a GET request against the RabbitMQ Management API.

        Args:
            path: API path (e.g. ``/queues``).

        Returns:
            Parsed JSON response.
        """
        url = self._management_api_url(path)
        response = httpx.get(
            url,
            auth=(self._rabbitmq_settings.username, self._rabbitmq_settings.password),
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()

    def _get_broker_queues(self) -> set[str]:
        """List all queue names from RabbitMQ via the Management API."""
        try:
            queues_data: list[dict[str, Any]] = self._management_api_get("/queues")
            return {queue["name"] for queue in queues_data if "name" in queue}
        except (httpx.HTTPError, KeyError):
            logger.warning("Could not list queues from Management API", exc_info=True)
            return set()

    def _get_exchange_bindings(self, exchange_name: str) -> list[dict[str, str]]:
        """List bindings for an exchange from the Management API.

        Returns:
            List of binding dicts with ``destination`` and ``routing_key`` keys.
        """
        try:
            bindings_data: list[dict[str, Any]] = self._management_api_get(
                f"/exchanges/%2F/{exchange_name}/bindings/source"
            )
            return [
                {"destination": binding["destination"], "routing_key": binding["routing_key"]}
                for binding in bindings_data
                if "destination" in binding and "routing_key" in binding
            ]
        except (httpx.HTTPError, KeyError):
            logger.warning(
                "Could not list bindings for exchange '%s' from Management API",
                exchange_name,
                exc_info=True,
            )
            return []

    # ------------------------------------------------------------------
    # Message creation and publishing
    # ------------------------------------------------------------------

    def create_message(  # pyright: ignore[reportUnknownParameterType]
        self,
        queue_name: str,
        actor_name: str,
        args: tuple[object, ...] = (),
        kwargs: dict[str, object] | None = None,
        options: dict[str, object] | None = None,
    ) -> dramatiq.Message:  # pyright: ignore[reportMissingTypeArgument]
        """Create a Dramatiq message for a given actor.

        Args:
            queue_name: Target queue name.
            actor_name: Dramatiq actor function name.
            args: Positional arguments for the actor.
            kwargs: Keyword arguments for the actor.
            options: Dramatiq message options (e.g. delay, max_retries).

        Returns:
            A Dramatiq Message ready for publishing.
        """
        return dramatiq.Message(  # pyright: ignore[reportCallIssue, reportUnknownVariableType]
            queue_name=queue_name,
            actor_name=actor_name,
            args=args,
            kwargs=kwargs or {},
            options=options or {},
        )

    def publish(  # pyright: ignore[reportUnknownParameterType, reportMissingTypeArgument]
        self,
        message: dramatiq.Message,  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        exchange_name: str = "",
        routing_key: str = "",
    ) -> None:
        """Publish a message to RabbitMQ with persistent delivery.

        Uses the management connection channel for publishing.  Messages
        are published with ``delivery_mode=2`` to survive RabbitMQ restarts.

        Args:
            message: Dramatiq message to publish.
            exchange_name: Target exchange (empty string for default exchange).
            routing_key: Routing key for topic exchange routing.
        """
        channel = self._management_connection.ensure_connected()
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key or message.queue_name,
            body=message.encode(),  # pyright: ignore[reportUnknownMemberType]
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        logger.debug(
            "Published message to exchange='%s' routing_key='%s' actor='%s'",
            exchange_name,
            routing_key or message.queue_name,
            message.actor_name,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def declare_queue(self, queue_name: str, durable: bool = True) -> None:
        """Declare a single queue on RabbitMQ.

        Convenience method for examples and ad-hoc queue creation.

        Args:
            queue_name: Name of the queue to declare.
            durable: Whether the queue survives broker restarts.
        """
        channel = self._management_connection.ensure_connected()
        channel.queue_declare(queue=queue_name, durable=durable)  # pyright: ignore[reportUnknownMemberType]

    def declare_exchange_and_bind(
        self,
        exchange: TopicExchange,
        queues: list[TopicExchangeQueue],
    ) -> None:
        """Declare an exchange and bind queues to it.

        Convenience method for examples and ad-hoc topology setup.

        Args:
            exchange: The exchange to declare.
            queues: Queues to declare and bind to the exchange.
        """
        channel = self._management_connection.ensure_connected()
        channel.exchange_declare(  # pyright: ignore[reportUnknownMemberType]
            exchange=exchange.name,
            exchange_type=exchange.exchange_type.value,
            durable=exchange.durable,
            arguments=exchange.arguments if exchange.arguments else None,
        )
        for queue_definition in queues:
            channel.queue_declare(queue=queue_definition.name, durable=queue_definition.durable)  # pyright: ignore[reportUnknownMemberType]
            channel.queue_bind(  # pyright: ignore[reportUnknownMemberType]
                queue=queue_definition.name,
                exchange=exchange.name,
                routing_key=queue_definition.routing_key,
            )

    def connect(self) -> None:
        """Connect the management connection to RabbitMQ.

        Must be called before using ``declare_queue()``,
        ``declare_exchange_and_bind()``, or ``publish()`` when
        the broker was created with ``load=False``.
        """
        self._management_connection.connect()

    def close(self) -> None:
        """Close the management connection."""
        self._management_connection.close()
        logger.info("DramatiqBroker management connection closed")
