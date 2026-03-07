"""ORM models for the quotes scraper example."""

from sqlalchemy import Column, DateTime, String, Text, func

from data_collector.examples.scraping import SCHEMA
from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class ExampleQuote(Base):
    """Quote record scraped from quotes.toscrape.com."""

    __tablename__ = "example_quote"
    __table_args__ = {"schema": SCHEMA}

    id = auto_increment_column()
    text = Column(Text, nullable=False)
    author = Column(String(256), nullable=False)
    tags = Column(String(512))
    sha = Column(String(64), nullable=False, index=True, comment="Row hash for merge-based sync")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())
    date_modified = Column(DateTime, onupdate=func.now())
