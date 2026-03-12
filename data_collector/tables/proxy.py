"""Proxy management ORM tables.

ProxyReservation tracks active IP reservations per root domain.
ProxyBlacklist tracks problematic IPs per subdomain with incremental lockout.

These are transient operational tables -- they do not follow DataTableMixin
(no sha, archive, date_created, date_modified columns).
"""

from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String, text

from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class ProxyReservation(Base):
    """Active IP reservation preventing same-IP collisions on a root domain.

    The unique partial index on (ip_address, target_domain) WHERE released = false
    guarantees no two active reservations for the same IP + domain, regardless
    of application-level timing. This is the ultimate safety net against race
    conditions across threads, processes, and servers.
    """

    __tablename__ = "proxy_reservation"

    id = auto_increment_column()
    ip_address = Column(String(45), nullable=False)
    target_domain = Column(String(255), nullable=False)
    app_id = Column(String(64), nullable=False)
    reserved_at = Column(DateTime, nullable=False)
    ttl_seconds = Column(Integer, default=1800)
    released = Column(Boolean, default=False, nullable=False)
    released_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index(
            "uq_proxy_reservation_ip_address",
            "ip_address",
            "target_domain",
            unique=True,
            postgresql_where=text("released = false"),
            mssql_where=text("released = 0"),
        ),
    )


class ProxyBlacklist(Base):
    """IP ban and lockout tracking per subdomain.

    Records problematic proxy IPs with incremental lockout durations.
    Used during proxy acquisition to skip known-bad IPs before reserving.

    Lockout progression (default):
        Level 0: 5 minutes
        Level 1: 20 minutes
        Level 2: 1 hour
        Level 3: 4 hours
        Level 4: 24 hours
        Level 5+: Permanent ban (is_banned = True)
    """

    __tablename__ = "proxy_blacklist"

    id = auto_increment_column()
    ip_address = Column(String(45), nullable=False)
    target_domain = Column(String(255), nullable=False)
    failure_count = Column(Integer, default=1, nullable=False)
    first_failure_at = Column(DateTime, nullable=False)
    last_failure_at = Column(DateTime, nullable=False)
    lockout_until = Column(DateTime, nullable=True)
    lockout_level = Column(Integer, default=0, nullable=False)
    is_banned = Column(Boolean, default=False, nullable=False)

    __table_args__ = (
        Index(
            "uq_proxy_blacklist_ip_domain",
            "ip_address",
            "target_domain",
            unique=True,
        ),
    )
