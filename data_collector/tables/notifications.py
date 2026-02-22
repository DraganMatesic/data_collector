from sqlalchemy import Column, String, BigInteger, DateTime, func

from data_collector.tables.shared import Base


class CodebookAlertSeverity(Base):
    """
    Codebook for alert severity levels
    """
    __tablename__ = "c_alert_severity"
    id = Column(BigInteger, primary_key=True, comment="Alert severity ID")
    description = Column(String(128), comment="Alert severity description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())
