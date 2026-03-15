"""Dead letter actor for messages that exhausted all retry attempts.

Messages forwarded to the dead-letter queue are logged at ERROR level
and persisted to the ``DeadLetter`` table for manual inspection and
reprocessing.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import dramatiq

from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.pipeline import DeadLetter
from data_collector.utilities.database.main import Database

logger = logging.getLogger(__name__)


@dramatiq.actor(queue_name="dc_dead_letters")  # pyright: ignore[reportUntypedFunctionDecorator]
def log_dead_letter(message_data: dict[str, Any]) -> None:
    """Log a failed message for manual inspection and reprocessing.

    Creates a ``DeadLetter`` record with the full payload, error details,
    and traceback.  This actor is stateless and idempotent.

    Called by Dramatiq's ``on_retry_exhausted`` mechanism with
    ``message.asdict()`` as the single argument.  The message dict
    structure::

        {
            "queue_name": "dc_...",
            "actor_name": "...",
            "args": (...),
            "kwargs": {...},
            "options": {"traceback": "...", "retries": N, ...},
            "message_id": "...",
            "message_timestamp": ...,
        }

    Args:
        message_data: Serialized message dict from ``message.asdict()``.
    """
    options = message_data.get("options", {})
    traceback_text = options.get("traceback")

    # Extract error type and message from the last line of the traceback.
    error_type = None
    error_message = None
    if traceback_text:
        traceback_lines = traceback_text.strip().splitlines()
        if traceback_lines:
            last_line = traceback_lines[-1]
            if ": " in last_line:
                error_type, error_message = last_line.split(": ", 1)
            else:
                error_type = last_line

    # Log first -- even if the database write fails, the error is visible
    # in the service log and (if LoggingService is active) in Splunk.
    logger.error(
        "Dead letter received: queue=%s actor=%s error=%s: %s",
        message_data.get("queue_name"),
        message_data.get("actor_name"),
        error_type,
        error_message,
    )

    try:
        database = Database(MainDatabaseSettings())
        with database.create_session() as session:
            dead_letter_record = DeadLetter(
                queue_name=message_data.get("queue_name"),
                actor_name=message_data.get("actor_name"),
                message_args=json.dumps(message_data.get("args", []), default=str),
                message_kwargs=json.dumps(message_data.get("kwargs", {}), default=str),
                error_type=error_type,
                error_message=error_message,
                traceback=traceback_text,
                original_message_id=message_data.get("message_id"),
            )
            session.add(dead_letter_record)
            session.commit()
    except Exception:
        logger.exception(
            "Failed to persist dead letter to database: queue=%s actor=%s message_id=%s",
            message_data.get("queue_name"),
            message_data.get("actor_name"),
            message_data.get("message_id"),
        )
