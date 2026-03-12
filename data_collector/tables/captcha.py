"""Captcha codebook tables, solve attempt logging, and error details.

CaptchaLog records every captcha task submitted to a provider, tracking
cost, timing, status, and correctness feedback. CaptchaLogError stores
provider error details (error code, description, category) in a separate
one-to-one table, following the FunctionLog/FunctionLogError split pattern.

These are operational log tables -- they do not follow DataTableMixin
(no sha, archive, date_modified columns).

CodebookCaptchaSolveStatus and CodebookCaptchaErrorCategory are standard
codebook tables seeded from the corresponding IntEnums in
``data_collector.enums.captcha``.
"""

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Index, String, Text, text
from sqlalchemy.sql import func

from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class CodebookCaptchaSolveStatus(Base):
    """Codebook for captcha solve attempt outcomes."""

    __tablename__ = "c_captcha_solve_status"
    id = Column(BigInteger, primary_key=True, comment="Solve status ID")
    description = Column(String(128), comment="Solve status description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())


class CodebookCaptchaErrorCategory(Base):
    """Codebook for provider-agnostic captcha error classifications."""

    __tablename__ = "c_captcha_error_category"
    id = Column(BigInteger, primary_key=True, comment="Error category ID")
    description = Column(String(128), comment="Error category description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())


class CaptchaLog(Base):
    """Captcha solve attempt record for analytics and cost tracking.

    Each row represents one task submitted to a captcha provider. The
    ``is_correct`` column starts as NULL (unknown) and is updated by
    ``report_correct()`` / ``report_incorrect()`` after the scraper
    verifies the solution against the target page.

    Provider error details (error code, description, category) are stored
    in the related ``CaptchaLogError`` table, not on this record.

    Analytics use cases:
        - Solve speed distribution per provider, task type, and domain
        - Cost trends over time (hourly, daily, per-domain)
        - Per-domain correctness rates for quality monitoring
        - Error pattern detection (which domains trigger proxy errors, etc.)
    """

    __tablename__ = "captcha_log"

    id = auto_increment_column()
    app_id = Column(String(64), ForeignKey("apps.app", ondelete="CASCADE"), index=True, nullable=False)
    runtime = Column(String(64), ForeignKey("runtime.runtime", ondelete="CASCADE"), index=True, nullable=False)
    provider_name = Column(String(64), nullable=False)
    task_id = Column(String(128), nullable=False)
    task_type = Column(String(32), nullable=False)
    page_url = Column(Text, nullable=False)
    cost: Column[float] = Column(Float, server_default=text("0"), nullable=False)
    elapsed_seconds: Column[float] = Column(Float, nullable=False)
    status = Column(
        BigInteger,
        ForeignKey("c_captcha_solve_status.id", ondelete="CASCADE"),
        nullable=False,
    )
    is_correct = Column(
        Boolean, nullable=True,
        comment="Whether the solved token was accepted by the target site. "
        "NULL=not yet verified, True=accepted, False=rejected. "
        "Set via report_correct/report_incorrect after submission.",
    )
    date_created = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_captcha_log_task_id", "task_id"),
    )


class CaptchaLogError(Base):
    """Provider error details for failed captcha solve attempts.

    One-to-one with CaptchaLog. A row is created only when the provider
    returns an error (status FAILED or TIMED_OUT). Successful solves have
    no CaptchaLogError record.

    Follows the FunctionLog/FunctionLogError split pattern
    (``data_collector.tables.log``).
    """

    __tablename__ = "captcha_log_error"

    id = auto_increment_column()
    captcha_log_id = Column(
        BigInteger,
        ForeignKey(CaptchaLog.id, ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    error_code = Column(String(128), nullable=True, doc="Provider error code")
    error_description = Column(Text, nullable=True, doc="Provider error message")
    error_category = Column(
        BigInteger,
        ForeignKey("c_captcha_error_category.id", ondelete="CASCADE"),
        nullable=True,
    )
    date_created = Column(DateTime, server_default=func.now())
