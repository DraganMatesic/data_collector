"""Proxy IP blacklist with incremental lockout and retention cleanup."""

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, delete, select

from data_collector.tables.proxy import ProxyBlacklist
from data_collector.utilities.database.main import Database

logger = logging.getLogger(__name__)

DEFAULT_LOCKOUT_DURATIONS: tuple[timedelta, ...] = (
    timedelta(minutes=5),
    timedelta(minutes=20),
    timedelta(hours=1),
    timedelta(hours=4),
    timedelta(hours=24),
)


class BlacklistChecker:
    """Manages proxy IP blacklist with incremental lockout per subdomain.

    Tracks problematic proxy IPs that have been banned or rate-limited on
    specific subdomains. Each failure escalates the lockout duration. After
    exceeding the maximum lockout level, the IP is permanently banned for
    that subdomain.

    Args:
        database: Database instance for session management.
        target_domain: Full subdomain for blacklist scoping (e.g., "sub.gov.hr").
        lockout_durations: Tuple of timedelta values for each lockout level.
            Exceeding the last level triggers a permanent ban.
        retention_days: Days to retain non-banned entries after last failure
            before cleanup removes them.
    """

    def __init__(
        self,
        database: Database,
        target_domain: str,
        lockout_durations: tuple[timedelta, ...] | None = None,
        retention_days: int = 30,
    ) -> None:
        self.database = database
        self.target_domain = target_domain
        self.lockout_durations = lockout_durations if lockout_durations is not None else DEFAULT_LOCKOUT_DURATIONS
        self.retention_days = retention_days

    def is_locked_out(self, ip_address: str) -> bool:
        """Check if an IP is currently locked out or permanently banned.

        Args:
            ip_address: The proxy IP address to check.

        Returns:
            True if the IP is banned or has an active lockout for this domain.
        """
        now = datetime.now(UTC)
        with self.database.create_session() as session:
            statement = select(ProxyBlacklist).where(
                and_(
                    ProxyBlacklist.ip_address == ip_address,
                    ProxyBlacklist.target_domain == self.target_domain,
                )
            )
            row = session.execute(statement).scalar_one_or_none()
            if row is None:
                return False
            if row.is_banned:  # type: ignore[truthy-bool]
                return True  # type: ignore[return-value]
            return bool(row.lockout_until is not None and row.lockout_until > now)  # type: ignore[operator]

    def record_failure(self, ip_address: str) -> None:
        """Record a proxy failure and escalate the lockout level.

        If the IP + domain combination already exists, increments failure_count
        and bumps lockout_level. If the lockout level exceeds the configured
        durations, the IP is permanently banned for this subdomain.

        Args:
            ip_address: The proxy IP that failed.
        """
        now = datetime.now(UTC)
        with self.database.create_session() as session:
            statement = select(ProxyBlacklist).where(
                and_(
                    ProxyBlacklist.ip_address == ip_address,
                    ProxyBlacklist.target_domain == self.target_domain,
                )
            )
            row = session.execute(statement).scalar_one_or_none()

            if row is None:
                lockout_until = now + self.lockout_durations[0] if self.lockout_durations else None
                new_entry = ProxyBlacklist(
                    ip_address=ip_address,
                    target_domain=self.target_domain,
                    failure_count=1,
                    first_failure_at=now,
                    last_failure_at=now,
                    lockout_until=lockout_until,
                    lockout_level=0,
                    is_banned=False,
                )
                session.add(new_entry)
            else:
                row.failure_count = row.failure_count + 1  # type: ignore[assignment]
                row.last_failure_at = now  # type: ignore[assignment]
                new_level = row.lockout_level + 1

                if new_level >= len(self.lockout_durations):  # type: ignore[operator]
                    row.is_banned = True  # type: ignore[assignment]
                    row.lockout_until = None  # type: ignore[assignment]
                    row.lockout_level = new_level  # type: ignore[assignment]
                    logger.warning(
                        "Proxy IP permanently banned",
                        extra={
                            "ip_address": ip_address,
                            "target_domain": self.target_domain,
                            "failure_count": row.failure_count,
                        },
                    )
                else:
                    row.lockout_level = new_level  # type: ignore[assignment]
                    row.lockout_until = now + self.lockout_durations[new_level]  # type: ignore[assignment]

            session.commit()

    def cleanup_expired(self) -> int:
        """Remove non-banned blacklist entries older than the retention period.

        Deletes entries where ``last_failure_at`` is older than
        ``retention_days`` and the IP is not permanently banned. This prevents
        the blacklist table from growing unboundedly.

        Returns:
            Number of deleted rows.
        """
        cutoff = datetime.now(UTC) - timedelta(days=self.retention_days)
        with self.database.create_session() as session:
            statement = delete(ProxyBlacklist).where(
                and_(
                    ProxyBlacklist.target_domain == self.target_domain,
                    ProxyBlacklist.is_banned == False,  # noqa: E712
                    ProxyBlacklist.last_failure_at < cutoff,
                )
            )
            result = session.execute(statement)
            deleted_count: int = result.rowcount  # type: ignore[assignment]
            session.commit()
            if deleted_count > 0:
                extra: dict[str, int | str] = {"deleted_count": deleted_count, "target_domain": self.target_domain}
                logger.info("Cleaned up expired blacklist entries", extra=extra)
            return int(deleted_count)
