"""Database-backed rate limiter backend for Dramatiq.

Implements Dramatiq's ``RateLimiterBackend`` interface using SQLAlchemy
ORM operations against the ``rate_limiter_state`` table.  Supports both
PostgreSQL and MSSQL via standard SQL (no database-specific features).

Provides the backend for:
- ``ConcurrentRateLimiter``: mutex (limit=1) and concurrency caps
- ``WindowRateLimiter``: time-windowed request limits (X requests per Y seconds)

Usage::

    from dramatiq.rate_limits import ConcurrentRateLimiter
    from data_collector.dramatiq.rate_limit_backend import DatabaseRateLimiterBackend

    backend = DatabaseRateLimiterBackend(Database(MainDatabaseSettings()))
    mutex = ConcurrentRateLimiter(backend, "ocr-processing", limit=1)

    @dramatiq.actor(queue_name="dc_ocr")
    def process_ocr(event_id: int) -> None:
        with mutex.acquire():
            ...
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from dramatiq.rate_limits.backend import RateLimiterBackend
from sqlalchemy import delete, func, select

from data_collector.tables.pipeline import RateLimiterState
from data_collector.utilities.database.main import Database


class DatabaseRateLimiterBackend(RateLimiterBackend):
    """Rate limiter backend using a relational database.

    All operations use row-level locking (``SELECT ... FOR UPDATE``)
    for atomicity.  Works with both PostgreSQL and MSSQL.

    Args:
        database: A ``Database`` instance for creating sessions.
    """

    def __init__(self, database: Database) -> None:
        self._database = database
        self._cleanup_interval = 300  # seconds between expired key cleanup
        self._last_cleanup = 0.0
        self._cleanup_lock = threading.Lock()

    def _ttl_to_expiry(self, ttl: int) -> datetime:
        """Convert a TTL in milliseconds to an absolute UTC expiry timestamp."""
        return datetime.now(UTC) + timedelta(milliseconds=ttl)

    def _cleanup_expired(self) -> None:
        """Remove expired keys periodically to prevent table bloat."""
        now = time.monotonic()
        if now - self._last_cleanup < self._cleanup_interval:
            return
        if not self._cleanup_lock.acquire(blocking=False):
            return
        try:
            with self._database.create_session() as session:
                session.execute(
                    delete(RateLimiterState).where(
                        RateLimiterState.expires_at < datetime.now(UTC),
                        RateLimiterState.expires_at.isnot(None),
                    )
                )
                session.commit()
            self._last_cleanup = now
        finally:
            self._cleanup_lock.release()

    def add(self, key: str, value: int, ttl: int) -> bool:
        """Add a key iff it does not already exist.

        Returns True if the key was added, False if it already exists
        (and has not expired).
        """
        self._cleanup_expired()
        with self._database.create_session() as session:
            existing = session.execute(
                select(RateLimiterState).where(RateLimiterState.key == key).with_for_update()
            ).scalar_one_or_none()

            if existing is not None:
                if existing.expires_at and existing.expires_at < datetime.now(UTC):  # pyright: ignore[reportGeneralTypeIssues]
                    existing.value = value  # pyright: ignore[reportAttributeAccessIssue]
                    existing.expires_at = self._ttl_to_expiry(ttl)  # pyright: ignore[reportAttributeAccessIssue]
                    session.commit()
                    return True
                return False

            session.add(RateLimiterState(
                key=key,
                value=value,
                expires_at=self._ttl_to_expiry(ttl),
            ))
            session.commit()
            return True

    def incr(self, key: str, amount: int, maximum: int, ttl: int) -> bool:
        """Atomically increment a key up to the given maximum.

        Returns True if the increment was successful (value <= maximum).
        """
        self._cleanup_expired()
        with self._database.create_session() as session:
            existing = session.execute(
                select(RateLimiterState).where(RateLimiterState.key == key).with_for_update()
            ).scalar_one_or_none()

            if existing is None:
                if amount > maximum:
                    return False
                session.add(RateLimiterState(
                    key=key,
                    value=amount,
                    expires_at=self._ttl_to_expiry(ttl),
                ))
                session.commit()
                return True

            if existing.expires_at and existing.expires_at < datetime.now(UTC):  # pyright: ignore[reportGeneralTypeIssues]
                existing.value = amount  # pyright: ignore[reportAttributeAccessIssue]
                existing.expires_at = self._ttl_to_expiry(ttl)  # pyright: ignore[reportAttributeAccessIssue]
                session.commit()
                return True

            new_value = existing.value + amount  # pyright: ignore[reportOperatorIssue]
            if new_value > maximum:  # pyright: ignore[reportGeneralTypeIssues]
                return False

            existing.value = new_value  # pyright: ignore[reportAttributeAccessIssue]
            existing.expires_at = self._ttl_to_expiry(ttl)  # pyright: ignore[reportAttributeAccessIssue]
            session.commit()
            return True

    def decr(self, key: str, amount: int, minimum: int, ttl: int) -> bool:
        """Atomically decrement a key down to the given minimum.

        Returns True if the decrement was successful (value >= minimum).
        """
        with self._database.create_session() as session:
            existing = session.execute(
                select(RateLimiterState).where(RateLimiterState.key == key).with_for_update()
            ).scalar_one_or_none()

            if existing is None:
                return False

            new_value = existing.value - amount  # pyright: ignore[reportOperatorIssue]
            if new_value < minimum:  # pyright: ignore[reportGeneralTypeIssues]
                return False

            existing.value = new_value  # pyright: ignore[reportAttributeAccessIssue]
            existing.expires_at = self._ttl_to_expiry(ttl)  # pyright: ignore[reportAttributeAccessIssue]
            session.commit()
            return True

    def incr_and_sum(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, key: str, keys: list[str] | Callable[[], list[str]], amount: int, maximum: int, ttl: int,
    ) -> bool:
        """Atomically increment a key unless the sum of related keys exceeds maximum.

        Used by ``WindowRateLimiter`` which creates time-bucketed keys.
        The ``keys`` parameter is a callable that returns the list of
        keys to sum over.

        Returns True if the increment was successful.
        """
        self._cleanup_expired()
        with self._database.create_session() as session:
            # Sum all related keys (excluding expired ones).
            key_list: list[str] = keys() if callable(keys) else keys
            current_sum_result = session.execute(
                select(func.coalesce(func.sum(RateLimiterState.value), 0)).where(
                    RateLimiterState.key.in_(key_list),
                    (RateLimiterState.expires_at > datetime.now(UTC)) | (RateLimiterState.expires_at.is_(None)),
                )
            ).scalar()
            current_sum = int(current_sum_result) if current_sum_result is not None else 0

            if current_sum + amount > maximum:
                return False

            # Upsert the specific key.
            existing = session.execute(
                select(RateLimiterState).where(RateLimiterState.key == key).with_for_update()
            ).scalar_one_or_none()

            if existing is None:
                session.add(RateLimiterState(
                    key=key,
                    value=amount,
                    expires_at=self._ttl_to_expiry(ttl),
                ))
            else:
                existing.value = existing.value + amount  # pyright: ignore[reportAttributeAccessIssue]
                existing.expires_at = self._ttl_to_expiry(ttl)  # pyright: ignore[reportAttributeAccessIssue]

            session.commit()
            return True

    def wait(self, key: str, timeout: int) -> bool:
        """Wait for a notification on the given key.

        Database-backed implementation uses polling with exponential backoff.
        Returns True if a notification was received before the timeout.
        """
        deadline = time.monotonic() + timeout / 1000.0
        wait_time = 0.1  # Start with 100ms polling interval
        while time.monotonic() < deadline:
            with self._database.create_session() as session:
                result = session.execute(
                    select(RateLimiterState).where(
                        RateLimiterState.key == f"notify:{key}",
                    )
                ).scalar_one_or_none()
                if result is not None:
                    session.execute(
                        delete(RateLimiterState).where(RateLimiterState.key == f"notify:{key}")
                    )
                    session.commit()
                    return True
            time.sleep(min(wait_time, max(0, deadline - time.monotonic())))
            wait_time = min(wait_time * 2, 1.0)  # Cap at 1 second
        return False

    def wait_notify(self, key: str, ttl: int) -> None:
        """Notify parties waiting on a key that an event has occurred."""
        notify_key = f"notify:{key}"
        with self._database.create_session() as session:
            existing = session.execute(
                select(RateLimiterState).where(RateLimiterState.key == notify_key)
            ).scalar_one_or_none()
            if existing is None:
                session.add(RateLimiterState(
                    key=notify_key,
                    value=1,
                    expires_at=self._ttl_to_expiry(ttl),
                ))
            else:
                existing.value = 1  # pyright: ignore[reportAttributeAccessIssue]
                existing.expires_at = self._ttl_to_expiry(ttl)  # pyright: ignore[reportAttributeAccessIssue]
            session.commit()
