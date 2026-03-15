"""Three-stage pipeline chaining example for examples.dramatiq.chaining.

Demonstrates multi-stage Dramatiq pipeline chaining where each stage
receives a message, logs its processing, and publishes to the next
stage via routing key on a shared topic exchange.

Stages: prepare -> process -> validate.
No real processing -- pure chaining verification with full
``@fun_watch`` observability.

How to run:
    1. Start RabbitMQ and the Dramatiq service
    2. Send a message::

        python -m data_collector.examples.dramatiq.chaining.send_test
        python -m data_collector.examples.dramatiq.chaining.send_test "my_payload"

    3. Check the Logs table for stage entries
    4. Check FunctionLog for ``@fun_watch`` rows per stage
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import dramatiq
import structlog

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.enums import AppType
from data_collector.enums.runtime import FatalFlag, RunStatus
from data_collector.settings.main import MainDatabaseSettings
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchMixin, fun_watch
from data_collector.utilities.functions.runtime import AppInfo, get_app_info

from .topics import MAIN_EXCHANGE_QUEUE, PROCESS_QUEUE, VALIDATE_QUEUE

_APP_INFO: AppInfo = get_app_info(__file__, depth=-4)  # type: ignore[assignment]
_APP_ID: str = _APP_INFO["app_id"]
_DATABASE = Database(MainDatabaseSettings())


def _register_app(database: Database, app_info: AppInfo) -> None:
    """Register AppGroups, AppParents, and Apps rows using idempotent update_insert."""
    with database.create_session() as session:
        database.update_insert(AppGroups(name=app_info["app_group"]), session, filter_cols=["name"])

    with database.create_session() as session:
        database.update_insert(
            AppParents(name=app_info["app_parent"], group_name=app_info["app_group"], parent=app_info["parent_id"]),
            session,
            filter_cols=["name", "group_name"],
        )

    with database.create_session() as session:
        database.update_insert(
            Apps(
                app=app_info["app_id"],
                group_name=app_info["app_group"],
                parent_name=app_info["app_parent"],
                app_name=app_info["app_name"],
                parent_id=app_info["parent_id"],
                run_status=RunStatus.NOT_RUNNING,
                fatal_flag=FatalFlag.NONE,
                disable=True,
                app_type=AppType.DRAMATIQ,
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


_register_app(_DATABASE, _APP_INFO)


def _create_runtime() -> str:
    """Create a Runtime row for this actor invocation and return the runtime_id."""
    runtime_id = uuid.uuid4().hex
    with _DATABASE.create_session() as session:
        session.add(Runtime(
            runtime=runtime_id,
            app_id=_APP_ID,
            start_time=datetime.now(UTC),
        ))
        session.commit()
    return runtime_id


def _publish_to_next_stage(
    queue_name: str,
    actor_name: str,
    exchange_name: str,
    routing_key: str,
    payload: str,
) -> None:
    """Publish a message to the next pipeline stage via routing key."""
    broker = DramatiqBroker(RabbitMQSettings(), load=False)
    broker.connect()
    message = broker.create_message(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
        queue_name=queue_name,
        actor_name=actor_name,
        args=(payload,),
    )
    broker.publish(  # pyright: ignore[reportUnknownMemberType]
        message,
        exchange_name=exchange_name,
        routing_key=routing_key,
    )
    broker.close()


# ---------------------------------------------------------------------------
# Stage 1: Prepare
# ---------------------------------------------------------------------------

class PrepareProcessor(FunWatchMixin):
    """Stage 1 -- receives raw data, forwards to process stage."""

    def __init__(self, app_id: str, runtime: str) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = structlog.get_logger(__name__).bind(app_id=app_id, runtime=runtime)

    @fun_watch
    def run(self, payload: str) -> None:
        """Log receipt and forward to stage 2."""
        self.logger.info("[Stage 1 - Prepare] Received: %s", payload)
        prepared = f"prepared:{payload}"
        self.logger.info("[Stage 1 - Prepare] Forwarding to stage 2")
        _publish_to_next_stage(
            queue_name=PROCESS_QUEUE.name,
            actor_name=PROCESS_QUEUE.actor_name,
            exchange_name=PROCESS_QUEUE.exchange_name,
            routing_key=PROCESS_QUEUE.routing_key,
            payload=prepared,
        )
        self._fun_watch.mark_solved()


@dramatiq.actor(queue_name=MAIN_EXCHANGE_QUEUE.name, max_retries=0, on_retry_exhausted="log_dead_letter")
def chaining_prepare(payload: str) -> None:
    """Thin entry point for stage 1."""
    runtime_id = _create_runtime()
    processor = PrepareProcessor(app_id=_APP_ID, runtime=runtime_id)
    processor.run(payload)


# ---------------------------------------------------------------------------
# Stage 2: Process
# ---------------------------------------------------------------------------

class ProcessProcessor(FunWatchMixin):
    """Stage 2 -- receives prepared data, forwards to validate stage."""

    def __init__(self, app_id: str, runtime: str) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = structlog.get_logger(__name__).bind(app_id=app_id, runtime=runtime)

    @fun_watch
    def run(self, payload: str) -> None:
        """Log receipt and forward to stage 3."""
        self.logger.info("[Stage 2 - Process] Received: %s", payload)
        processed = f"processed:{payload}"
        self.logger.info("[Stage 2 - Process] Forwarding to stage 3")
        _publish_to_next_stage(
            queue_name=VALIDATE_QUEUE.name,
            actor_name=VALIDATE_QUEUE.actor_name,
            exchange_name=VALIDATE_QUEUE.exchange_name,
            routing_key=VALIDATE_QUEUE.routing_key,
            payload=processed,
        )
        self._fun_watch.mark_solved()


@dramatiq.actor(queue_name=PROCESS_QUEUE.name, max_retries=0, on_retry_exhausted="log_dead_letter")
def chaining_process(payload: str) -> None:
    """Thin entry point for stage 2."""
    runtime_id = _create_runtime()
    processor = ProcessProcessor(app_id=_APP_ID, runtime=runtime_id)
    processor.run(payload)


# ---------------------------------------------------------------------------
# Stage 3: Validate (final)
# ---------------------------------------------------------------------------

class ValidateProcessor(FunWatchMixin):
    """Stage 3 -- receives processed data, logs completion."""

    def __init__(self, app_id: str, runtime: str) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = structlog.get_logger(__name__).bind(app_id=app_id, runtime=runtime)

    @fun_watch
    def run(self, payload: str) -> None:
        """Log receipt -- final stage, no forwarding."""
        self.logger.info("[Stage 3 - Validate] Received: %s", payload)
        self.logger.info("[Stage 3 - Validate] Pipeline complete")
        self._fun_watch.mark_solved()


@dramatiq.actor(queue_name=VALIDATE_QUEUE.name, max_retries=0, on_retry_exhausted="log_dead_letter")
def chaining_validate(payload: str) -> None:
    """Thin entry point for stage 3."""
    runtime_id = _create_runtime()
    processor = ValidateProcessor(app_id=_APP_ID, runtime=runtime_id)
    processor.run(payload)
