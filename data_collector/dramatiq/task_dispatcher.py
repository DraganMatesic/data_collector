"""Database-driven event dispatcher for Dramatiq pipeline processing.

The ``TaskDispatcher`` polls the ``Events`` table for unprocessed events
and publishes Dramatiq messages to the correct exchange/queue based on
each event's ``app_path`` metadata.  Designed to run as a daemon thread
inside the Manager process.

Thread lifecycle follows the same pattern as
``data_collector.messaging.consumer.CommandConsumer``: ``start()`` spawns
a daemon thread, ``stop()`` sets a threading event and joins.
"""

from __future__ import annotations

import importlib
import logging
import threading
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.dramatiq.topic.base import TopicExchangeQueue
from data_collector.settings.dramatiq import TaskDispatcherSettings
from data_collector.tables.pipeline import EventProcessingStatus, Events
from data_collector.utilities.database.main import Database

logger = logging.getLogger(__name__)


class TaskDispatcher:
    """Polls Events table and dispatches to Dramatiq actors via RabbitMQ.

    The dispatcher runs as a daemon thread inside the Manager process.
    It queries for events that have no matching ``EventProcessingStatus``
    row (i.e. have not yet been dispatched), dynamically imports the
    event's ``app_path`` module to retrieve the ``TopicExchangeQueue``
    definition, creates a Dramatiq message, publishes it, and records
    the dispatch in ``EventProcessingStatus``.

    Args:
        database: Database instance for session creation.
        broker: DramatiqBroker for message creation and publishing.
        settings: Dispatcher configuration.  Uses defaults if not provided.
    """

    def __init__(
        self,
        database: Database,
        broker: DramatiqBroker,
        settings: TaskDispatcherSettings | None = None,
    ) -> None:
        self._database = database
        self._broker = broker
        resolved_settings = settings or TaskDispatcherSettings()
        self._batch_size = resolved_settings.batch_size
        self._poll_interval = resolved_settings.poll_interval
        self._yield_per = max(1, self._batch_size // 10)
        self._stop_event = threading.Event()
        self._dispatcher_thread: threading.Thread | None = None

    def start(self) -> None:
        """Start dispatching events in a background daemon thread.

        Raises:
            RuntimeError: If the dispatcher is already running.
        """
        if self._dispatcher_thread is not None and self._dispatcher_thread.is_alive():
            raise RuntimeError("TaskDispatcher is already running")

        self._stop_event.clear()
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop,
            daemon=True,
            name="task-dispatcher",
        )
        self._dispatcher_thread.start()
        logger.info("TaskDispatcher started (batch_size=%d, poll_interval=%ds)", self._batch_size, self._poll_interval)

    def stop(self, timeout: float = 10.0) -> None:
        """Signal the dispatcher to stop and wait for the thread to exit.

        Args:
            timeout: Maximum seconds to wait for thread shutdown.
        """
        self._stop_event.set()

        if self._dispatcher_thread is not None:
            self._dispatcher_thread.join(timeout=timeout)
            if self._dispatcher_thread.is_alive():
                logger.warning("TaskDispatcher thread did not exit within %.1f seconds", timeout)
            else:
                self._dispatcher_thread = None

        logger.info("TaskDispatcher stopped")

    @property
    def is_running(self) -> bool:
        """Whether the dispatcher thread is alive."""
        return self._dispatcher_thread is not None and self._dispatcher_thread.is_alive()

    # ------------------------------------------------------------------
    # Main dispatch loop
    # ------------------------------------------------------------------

    def _dispatch_loop(self) -> None:
        """Main loop running in the daemon thread.

        Repeatedly calls ``_dispatch_batch()``.  When no events are
        available, waits on the stop event for ``poll_interval`` seconds.
        On errors, logs and waits before retrying (same pattern as
        ``CommandConsumer._consume_loop``).
        """
        reconnect_delay = 1

        while not self._stop_event.is_set():
            try:
                dispatched = self._dispatch_batch()
                reconnect_delay = 1
                if dispatched == 0:
                    self._stop_event.wait(timeout=self._poll_interval)
            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception("Error in dispatch loop, retrying in %d seconds", reconnect_delay)
                self._stop_event.wait(timeout=reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    # ------------------------------------------------------------------
    # Batch dispatch
    # ------------------------------------------------------------------

    def _dispatch_batch(self) -> int:
        """Find and dispatch unprocessed events.

        Uses a COUNT-first strategy to avoid fetching rows when the
        queue is empty.  Events are fetched in batches with ``yield_per``
        for memory efficiency.

        Returns:
            Number of events successfully dispatched.
        """
        with self._database.create_session() as session:
            count_query = (
                select(func.count())
                .select_from(Events)
                .outerjoin(
                    EventProcessingStatus,
                    Events.id == EventProcessingStatus.event_id,
                )
                .where(
                    Events.archive.is_(None),
                    EventProcessingStatus.id.is_(None),
                )
            )
            pending_count = self._database.query(count_query, session).scalar()

            if not pending_count:
                return 0

            events_query = (
                select(Events)
                .outerjoin(
                    EventProcessingStatus,
                    Events.id == EventProcessingStatus.event_id,
                )
                .where(
                    Events.archive.is_(None),
                    EventProcessingStatus.id.is_(None),
                )
                .limit(self._batch_size)
            )
            events = self._database.query(events_query, session).yield_per(self._yield_per).scalars()

            dispatched = 0
            for event in events:
                if self._dispatch_event(event, session):
                    dispatched += 1

            logger.info("Dispatched %d/%d events", dispatched, pending_count)
            return dispatched

    def _dispatch_event(self, event: Events, session: Session) -> bool:
        """Dispatch a single event to the correct Dramatiq actor.

        Dynamically imports the module at ``event.app_path`` and reads
        the ``MAIN_EXCHANGE_QUEUE`` constant (a ``TopicExchangeQueue``)
        to determine the target exchange, queue, and routing key.

        Args:
            event: The Events ORM instance to dispatch.
            session: SQLAlchemy session for recording EventProcessingStatus.

        Returns:
            True if the event was successfully dispatched.
        """
        try:
            module = importlib.import_module(str(event.app_path))
            queue_definition: TopicExchangeQueue = cast(Any, module).MAIN_EXCHANGE_QUEUE

            message = cast(Any, self._broker).create_message(
                queue_name=queue_definition.name,
                actor_name=queue_definition.actor_name,
                args=(event.id,),
            )
            cast(Any, self._broker).publish(
                message,
                exchange_name=queue_definition.exchange_name,
                routing_key=queue_definition.routing_key,
            )

            processing_status = EventProcessingStatus(
                event_id=event.id,
                actor_name=queue_definition.actor_name,
                dispatched_date=datetime.now(UTC),
            )
            self._database.add(processing_status, session)
            session.commit()

            logger.debug(
                "Dispatched event %s to actor '%s' via routing_key='%s'",
                event.id,
                queue_definition.actor_name,
                queue_definition.routing_key,
            )
            return True

        except ImportError:
            logger.error("Could not import app_path module: %s (event_id=%s)", event.app_path, event.id)
            return False
        except AttributeError:
            logger.error(
                "Module '%s' has no MAIN_EXCHANGE_QUEUE constant (event_id=%s)",
                event.app_path,
                event.id,
            )
            return False
        except Exception:
            logger.exception("Failed to dispatch event %s", event.id)
            session.rollback()
            return False
