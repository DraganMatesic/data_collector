"""Proxy acquisition, reservation, and lifecycle management."""

import logging
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, delete, select, update
from sqlalchemy.exc import IntegrityError

from data_collector.proxy.blacklist import BlacklistChecker
from data_collector.proxy.judges import PROXY_JUDGES, verify_ip
from data_collector.proxy.models import Proxy, ProxyAcquisitionTimeout, extract_root_domain
from data_collector.proxy.provider import ProxyProvider
from data_collector.tables.proxy import ProxyReservation
from data_collector.utilities.database.main import Database

logger = logging.getLogger(__name__)


class ProxyManager:
    """Orchestrates proxy acquisition, IP reservation, and lifecycle management.

    Combines a ProxyProvider (URL construction), proxy judges (IP verification),
    database reservation (collision prevention), and blacklist checking
    (ban avoidance) into a single acquisition flow.

    Reservation uses root domain (e.g., "gov.de") to prevent IP collision
    across all subdomains. Blacklist uses full subdomain (e.g., "sub.gov.de")
    because a ban on one subdomain does not imply a ban on others.

    A daemon cleanup thread runs periodically to delete this app's released
    reservations that are past the cooldown period. The thread dies automatically
    when the process exits. Call ``stop()`` for graceful shutdown.

    Args:
        provider: Proxy service provider for URL construction.
        database: Database instance for reservation and blacklist tables.
        target_domain: Full target domain including subdomain (e.g., "sub.gov.de").
        app_id: The 64-char SHA-256 app identifier.
        judges: Proxy judge URLs for IP verification. Defaults to PROXY_JUDGES.
        ttl_seconds: Reservation TTL in seconds (default 1800 = 30 minutes).
        cooldown_seconds: Seconds before a released IP can be reused (default 300 = 5 minutes).
        acquire_timeout: Maximum seconds to wait for a unique proxy (default 120).
        recheck_interval: Seconds between retries when IP is reserved (default 5).
        cleanup_interval: Seconds between daemon cleanup runs. Defaults to ``cooldown_seconds``.
        lockout_durations: Lockout progression for blacklist. None uses defaults.
        blacklist_retention_days: Days to retain non-banned blacklist entries (default 30).
    """

    def __init__(
        self,
        provider: ProxyProvider,
        database: Database,
        target_domain: str,
        app_id: str,
        judges: list[str] | tuple[str, ...] | None = None,
        ttl_seconds: int = 1800,
        cooldown_seconds: int = 300,
        acquire_timeout: int = 120,
        recheck_interval: int = 5,
        cleanup_interval: int | None = None,
        lockout_durations: tuple[timedelta, ...] | None = None,
        blacklist_retention_days: int = 30,
    ) -> None:
        self.provider = provider
        self.database = database
        self.target_domain = target_domain
        self.reservation_domain = extract_root_domain(target_domain)
        self.app_id = app_id
        self.judges = judges if judges is not None else PROXY_JUDGES
        self.ttl_seconds = ttl_seconds
        self.cooldown_seconds = cooldown_seconds
        self.acquire_timeout = acquire_timeout
        self.recheck_interval = recheck_interval
        self.cleanup_interval = cleanup_interval if cleanup_interval is not None else cooldown_seconds

        self.blacklist_checker = BlacklistChecker(
            database=database,
            target_domain=target_domain,
            lockout_durations=lockout_durations,
            retention_days=blacklist_retention_days,
        )

        self._stop_event = threading.Event()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name=f"proxy-cleanup-{self.app_id[:8]}",
        )
        self._cleanup_thread.start()

    def acquire(self, logger: logging.Logger) -> Proxy:
        """Acquire a unique, reserved proxy for this domain.

        The acquisition loop:
        1. Check provider health (on first attempt).
        2. Generate a new session ID and build the proxy URL.
        3. Verify the assigned IP via proxy judges.
        4. Check the blacklist for active lockout on this subdomain.
        5. Atomically reserve the IP for the root domain.
        6. Return the Proxy object on success.

        Args:
            logger: Logger for acquisition status messages.

        Returns:
            Proxy object with verified IP, URL, session ID, and target domain.

        Raises:
            ProxyAcquisitionTimeout: If no unique proxy found within timeout.
        """
        start = time.monotonic()
        health_checked = False

        while (time.monotonic() - start) < self.acquire_timeout:
            if not health_checked:
                if not self.provider.is_healthy():
                    logger.warning("Proxy provider health check failed, retrying")
                    time.sleep(self.recheck_interval)
                    continue
                health_checked = True

            session_id = uuid.uuid4().hex
            proxy_url = self.provider.build_proxy_url(session_id)

            ip_address = verify_ip(proxy_url, self.judges)
            if ip_address is None:
                logger.warning("All proxy judges failed, retrying with new session")
                continue

            if self.blacklist_checker.is_locked_out(ip_address):
                logger.debug(
                    "IP blacklisted for this subdomain, skipping",
                    extra={"ip_address": ip_address, "target_domain": self.target_domain},
                )
                continue

            if self._try_reserve(ip_address):
                logger.info(
                    "Proxy acquired",
                    extra={
                        "ip_address": ip_address,
                        "reservation_domain": self.reservation_domain,
                        "target_domain": self.target_domain,
                    },
                )
                return Proxy(
                    url=proxy_url,
                    ip_address=ip_address,
                    session_id=session_id,
                    target_domain=self.target_domain,
                )

            logger.debug(
                "IP already reserved for domain, retrying",
                extra={"ip_address": ip_address, "reservation_domain": self.reservation_domain},
            )
            time.sleep(self.recheck_interval)

        raise ProxyAcquisitionTimeout(
            f"No unique proxy acquired for {self.target_domain} "
            f"(reservation domain: {self.reservation_domain}) "
            f"within {self.acquire_timeout}s"
        )

    def release(self, ip_address: str) -> None:
        """Release a proxy reservation on thread/session completion.

        Marks the active reservation for this IP + app_id as released
        and records the release timestamp for cooldown tracking.

        Args:
            ip_address: The proxy IP address to release.
        """
        with self.database.create_session() as session:
            statement = (
                update(ProxyReservation)
                .where(
                    and_(
                        ProxyReservation.ip_address == ip_address,
                        ProxyReservation.target_domain == self.reservation_domain,
                        ProxyReservation.app_id == self.app_id,
                        ProxyReservation.released == False,  # noqa: E712
                    )
                )
                .values(released=True, released_at=datetime.now(UTC))
            )
            self.database.run(statement, session)
            session.commit()

    def report_failure(self, ip_address: str) -> None:
        """Report a proxy failure for blacklist tracking.

        Called by scraper code when target requests fail with proxy-related
        errors (403, CAPTCHA trigger, connection refused). Records the failure
        against the full subdomain and escalates the lockout level.

        Args:
            ip_address: The proxy IP that experienced a failure.
        """
        self.blacklist_checker.record_failure(ip_address)

    def _try_reserve(self, ip_address: str) -> bool:
        """Atomically reserve an IP for the root domain.

        Three-layer defense:
        1. UPDATE expired reservations (TTL-based crash safety).
        2. CHECK cooldown -- skip IPs released within ``cooldown_seconds``.
        3. INSERT new reservation -- unique partial index is the arbiter.

        No SELECT FOR UPDATE, no row locks held across statements. The UPDATE
        only touches expired rows. The INSERT either succeeds atomically or
        fails with IntegrityError immediately.

        Args:
            ip_address: The verified proxy IP to reserve.

        Returns:
            True if reservation succeeded, False if IP already taken or in cooldown.
        """
        now = datetime.now(UTC)
        ttl_cutoff = now - timedelta(seconds=self.ttl_seconds)
        cooldown_cutoff = now - timedelta(seconds=self.cooldown_seconds)

        with self.database.create_session() as session:
            expire_statement = (
                update(ProxyReservation)
                .where(
                    and_(
                        ProxyReservation.ip_address == ip_address,
                        ProxyReservation.target_domain == self.reservation_domain,
                        ProxyReservation.released == False,  # noqa: E712
                        ProxyReservation.reserved_at < ttl_cutoff,
                    )
                )
                .values(released=True, released_at=now)
            )
            self.database.run(expire_statement, session)

            cooldown_check = (
                select(ProxyReservation.id)
                .where(
                    and_(
                        ProxyReservation.ip_address == ip_address,
                        ProxyReservation.target_domain == self.reservation_domain,
                        ProxyReservation.released == True,  # noqa: E712
                        ProxyReservation.released_at > cooldown_cutoff,
                    )
                )
                .limit(1)
            )
            if self.database.query(cooldown_check, session).first() is not None:
                return False

            try:
                reservation = ProxyReservation(
                    ip_address=ip_address,
                    target_domain=self.reservation_domain,
                    app_id=self.app_id,
                    reserved_at=now,
                    ttl_seconds=self.ttl_seconds,
                )
                self.database.add(reservation, session)
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

    def _cleanup_loop(self) -> None:
        """Daemon thread loop: periodically clean own released reservations.

        Runs until ``_stop_event`` is set. Exceptions are caught to prevent
        the daemon thread from crashing; residual rows are handled by the
        janitor CLI (``cleanup_all_reservations``).
        """
        while not self._stop_event.wait(timeout=self.cleanup_interval):
            try:
                deleted = self.cleanup_reservations()
                if deleted > 0:
                    logger.debug(
                        "Cleanup thread deleted released reservations",
                        extra={"deleted_count": deleted, "app_id": self.app_id},
                    )
            except Exception:
                logger.warning("Cleanup thread encountered an error", exc_info=True)

    def cleanup_reservations(self) -> int:
        """Delete own released reservations past the cooldown period.

        Scoped to ``self.app_id`` -- each app cleans only its own rows.
        Called by the daemon cleanup thread periodically. Can also be
        called manually.

        Returns:
            Number of deleted rows.
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=self.cooldown_seconds)
        with self.database.create_session() as session:
            statement = delete(ProxyReservation).where(
                and_(
                    ProxyReservation.app_id == self.app_id,
                    ProxyReservation.released == True,  # noqa: E712
                    ProxyReservation.released_at < cutoff,
                )
            )
            result = self.database.run(statement, session)
            deleted_count = int(result.rowcount)  # type: ignore[arg-type]
            session.commit()
            return deleted_count

    def shutdown(self) -> None:
        """Flush all released reservations for this app and stop the daemon thread.

        Called during app cleanup. Deletes all released rows for this ``app_id``
        regardless of cooldown (the app is done, its own cooldown is irrelevant).
        Sibling apps are protected by the cooldown check in ``_try_reserve()``.
        """
        self._stop_event.set()
        with self.database.create_session() as session:
            statement = delete(ProxyReservation).where(
                and_(
                    ProxyReservation.app_id == self.app_id,
                    ProxyReservation.released == True,  # noqa: E712
                )
            )
            result = self.database.run(statement, session)
            deleted_count = int(result.rowcount)  # type: ignore[arg-type]
            session.commit()
        if deleted_count > 0:
            logger.debug(
                "Shutdown flushed released reservations",
                extra={"deleted_count": deleted_count, "app_id": self.app_id},
            )

    def stop(self) -> None:
        """Signal the daemon cleanup thread to stop.

        The daemon thread will exit after completing its current sleep cycle.
        This is optional -- the thread dies automatically when the process exits.
        Use ``shutdown()`` instead for app cleanup (flushes released rows first).
        """
        self._stop_event.set()


def cleanup_all_reservations(
    database: Database,
    cooldown_seconds: int = 300,
    ttl_seconds: int = 1800,
) -> int:
    """Janitor: delete all orphaned proxy reservations across all apps.

    Deletes two categories of dead rows:
    - ``released=True`` rows older than ``cooldown_seconds`` (past cooldown, any app).
    - ``released=False`` rows older than ``ttl_seconds`` (crash orphans from
      force-killed processes that never called ``release()``).

    Safe to run while apps are active: only touches rows that are demonstrably
    dead (past both cooldown and TTL thresholds).

    Args:
        database: Database instance with access to the proxy_reservation table.
        cooldown_seconds: Age threshold for released rows (default 300).
        ttl_seconds: Age threshold for unreleased crash orphans (default 1800).

    Returns:
        Total number of deleted rows.
    """
    now = datetime.now(UTC)
    cooldown_cutoff = now - timedelta(seconds=cooldown_seconds)
    ttl_cutoff = now - timedelta(seconds=ttl_seconds)

    total_deleted = 0

    with database.create_session() as session:
        released_statement = delete(ProxyReservation).where(
            and_(
                ProxyReservation.released == True,  # noqa: E712
                ProxyReservation.released_at < cooldown_cutoff,
            )
        )
        result = database.run(released_statement, session)
        total_deleted += int(result.rowcount)  # type: ignore[arg-type]
        session.commit()

    with database.create_session() as session:
        orphan_statement = delete(ProxyReservation).where(
            and_(
                ProxyReservation.released == False,  # noqa: E712
                ProxyReservation.reserved_at < ttl_cutoff,
            )
        )
        result = database.run(orphan_statement, session)
        total_deleted += int(result.rowcount)  # type: ignore[arg-type]
        session.commit()

    if total_deleted > 0:
        logger.info("Janitor cleaned up proxy reservations", extra={"deleted_count": total_deleted})

    return total_deleted
