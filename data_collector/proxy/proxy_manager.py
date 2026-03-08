"""Proxy acquisition, reservation, and lifecycle management."""

import logging
import time
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, update
from sqlalchemy.exc import IntegrityError

from data_collector.proxy.blacklist import BlacklistChecker
from data_collector.proxy.judges import PROXY_JUDGES, verify_ip
from data_collector.proxy.models import Proxy, ProxyAcquisitionTimeout, extract_root_domain
from data_collector.proxy.provider import ProxyProvider
from data_collector.tables.proxy import ProxyReservation
from data_collector.utilities.database.main import Database


class ProxyManager:
    """Orchestrates proxy acquisition, IP reservation, and lifecycle management.

    Combines a ProxyProvider (URL construction), proxy judges (IP verification),
    database reservation (collision prevention), and blacklist checking
    (ban avoidance) into a single acquisition flow.

    Reservation uses root domain (e.g., "gov.de") to prevent IP collision
    across all subdomains. Blacklist uses full subdomain (e.g., "sub.gov.de")
    because a ban on one subdomain does not imply a ban on others.

    Args:
        provider: Proxy service provider for URL construction.
        database: Database instance for reservation and blacklist tables.
        target_domain: Full target domain including subdomain (e.g., "sub.gov.de").
        app_id: The 64-char SHA-256 app identifier.
        judges: Proxy judge URLs for IP verification. Defaults to PROXY_JUDGES.
        ttl_seconds: Reservation TTL in seconds (default 1800 = 30 minutes).
        acquire_timeout: Maximum seconds to wait for a unique proxy (default 120).
        recheck_interval: Seconds between retries when IP is reserved (default 5).
        lockout_durations: Lockout progression for blacklist. None uses defaults.
        blacklist_retention_days: Days to retain non-banned blacklist entries (default 30).
    """

    def __init__(
        self,
        provider: ProxyProvider,
        database: Database,
        target_domain: str,
        app_id: str,
        judges: list[str] | None = None,
        ttl_seconds: int = 1800,
        acquire_timeout: int = 120,
        recheck_interval: int = 5,
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
        self.acquire_timeout = acquire_timeout
        self.recheck_interval = recheck_interval

        self.blacklist_checker = BlacklistChecker(
            database=database,
            target_domain=target_domain,
            lockout_durations=lockout_durations,
            retention_days=blacklist_retention_days,
        )

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

        Marks the active reservation for this IP + app_id as released.

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
                .values(released=True)
            )
            session.execute(statement)
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

        Two-layer defense:
        1. UPDATE expired reservations (TTL-based crash safety).
        2. INSERT new reservation -- unique partial index is the arbiter.

        No SELECT FOR UPDATE, no row locks held across statements. The UPDATE
        only touches expired rows. The INSERT either succeeds atomically or
        fails with IntegrityError immediately.

        Args:
            ip_address: The verified proxy IP to reserve.

        Returns:
            True if reservation succeeded, False if IP already taken.
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=self.ttl_seconds)

        with self.database.create_session() as session:
            expire_statement = (
                update(ProxyReservation)
                .where(
                    and_(
                        ProxyReservation.ip_address == ip_address,
                        ProxyReservation.target_domain == self.reservation_domain,
                        ProxyReservation.released == False,  # noqa: E712
                        ProxyReservation.reserved_at < cutoff,
                    )
                )
                .values(released=True)
            )
            session.execute(expire_statement)

            try:
                reservation = ProxyReservation(
                    ip_address=ip_address,
                    target_domain=self.reservation_domain,
                    app_id=self.app_id,
                    reserved_at=datetime.now(UTC),
                    ttl_seconds=self.ttl_seconds,
                )
                session.add(reservation)
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False
